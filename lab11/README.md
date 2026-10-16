# Lab 11 – Zerobus streaming 

Live AIS vessel traffic is pushed from a local producer straight into Unity Catalog Delta tables through Zerobus Ingest, with no message bus in between.

## Source

[aisstream.io](https://aisstream.io) streams global AIS messages (vessel positions and ship data) over a WebSocket, free with an API key.

What the exploration showed:

| Check | Result |
|---|---|
| Throughput, Marmara Sea box | 0.2–0.4 msg/s (sparse receiver coverage) |
| Throughput, whole world | 86–140 msg/s, densest in Europe |
| Message types | `PositionReport` 63%, `StandardClassBPositionReport` 17%, `ShipStaticData` 11%, `StaticDataReport` 8%, `ExtendedClassBPositionReport` <1% |
| Duplicates in the source | none; `time_utc` always in order |
| "Not available" sentinels | heading 511 in ~51% of positions, rate of turn −128 in ~32%, course 360 in ~25%, IMO 0 in ~48% of static data |
| Event time | `MetaData.time_utc`, nanoseconds with trailing zeros dropped, non-ISO suffix (`+0000 UTC`) |
| Replay | none: events missed while disconnected are lost |

Chosen approach:

- One subscription for the whole world, filtered on the server to the 5 data types.
- Two target tables: `bronze_positions` (3 position types) and `bronze_ship_static` (2 static types). `ExtendedClassBPositionReport` carries both and goes to both.
- The producer normalizes messages before sending: sentinels become `NULL`, strings are trimmed, and time becomes epoch microseconds. The original message is kept in `raw_payload VARIANT`, because the source cannot replay.
- `event_id` is a SHA-256 of the message content (type, MMSI, `time_utc`, coordinates or report part). Any producer computes the same id for the same event.

## Why protobuf

Zerobus accepts JSON, protobuf and Arrow over gRPC.

- **Protobuf**: typed and compact, and the documented recommendation for row-oriented production streams. A record is 177 bytes against 633 bytes of source JSON (~761 bytes with `raw_payload`).
- **JSON** was rejected: it is untyped and larger. Its only advantage, the rescue column, catches fields that do not fit the schema, but it is Beta.
- **Arrow** was rejected: it targets columnar batches, while AIS events arrive one at a time.

## Why two streams

- A Zerobus stream writes to exactly one table, so the two tables need two streams.
- Both streams come from one `ZerobusSdk` instance and share one HTTP/2 connection (`connection_per_stream: false`). The SDK recommends this for low-throughput streams; the measured rates are ~110 and ~28 rec/s.

## Asynchronous communication

- Records are sent with `ingest_record_offset()`, which returns once the record is queued. The producer never waits for a single record.
- Acknowledgments arrive in the background: `AckCallback.on_ack` / `on_error` update the `acked` and `failed` counters.
- Durability is confirmed only at shutdown: `close()` flushes both streams.
- `max_inflight_records = 10000` (SDK default 1,000,000) bounds the unacknowledged buffer. If Zerobus stops acknowledging, ingestion blocks instead of growing memory.
- Measured: ack ~0.3 s, about 28 records in flight at ~120 rec/s, 2–6 s until rows are queryable.

## Message types and schema management

- The table is the contract. The `.proto` files are generated from the tables (`build_proto.py` runs `generate_proto`, then compiles), so the schema cannot drift.
- Type mapping:
  - `TIMESTAMP` -> `int64` epoch µs;
  - `VARIANT` -> `string` with JSON;
  - `NOT NULL` columns -> `required`, the rest `optional`.
- Key columns are `NOT NULL`: `event_id`, `message_type`, `mmsi`, `event_ts`, `ingest_ts`, `producer_id`, `producer_seq`. All other columns are nullable, and `None` values are not sent, so they land as `NULL`.
- Protobuf has no rescue column, so the normalizer must produce exact types.
- Schema evolution followed the documented safe path: `02_add_raw_payload.sql` added a nullable `raw_payload` column, then the `.proto` files were regenerated and the producer updated. Target tables are never recreated, which Zerobus does not support.

## Recovery and retry patterns

Zerobus delivers at least once, so retries are allowed to create duplicates, and the lakehouse removes them.

| Layer | Behaviour |
|---|---|
| Zerobus SDK | automatic stream recovery on transient failures (4 retries, 2 s backoff, 15 s timeout) |
| Producer, stream failure (`ZerobusException`) | `get_unacked_records()` -> `recreate_stream()` resends unacknowledged records (counted as `replayed`), then retries the current record (3 attempts, waiting 2 s and 4 s between them) |
| Producer, aisstream disconnect | reconnect with exponential backoff 1 -> 60 s; the gap is logged as lost |
| Detection | per table stream, `producer_seq` increases by 1, so a repeated value means a replay and a missing value means a loss |
| Deduplication | bronze stays append-only; silver is loaded with `MERGE ... WHEN NOT MATCHED THEN INSERT` on `event_id`, keeping the first copy |

## Error handling

| Error | Where | Handling |
|---|---|---|
| `NonRetriableException` (bad credentials, missing table, no privileges) | stream open / ingest | stop the producer; streams are closed in `finally` |
| `ZerobusException` (network, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`) | ingest | recovery and retries as above |
| `on_error` callback | per record, after send | counted as `failed` and logged; `INVALID_PARAMETER_VALUE` here means the normalizer produced a value that does not fit the schema |
| Malformed AIS message | normalizer | counted as `malformed`, skipped; the stream continues |
| aisstream rejects the API key | source | stop; retrying cannot help |
| other aisstream errors / disconnects | source | reconnect with backoff |

## Networking considerations

- Only outbound connections over TLS: WebSocket to aisstream and gRPC to `https://<workspace-id>.zerobus.eastus.azuredatabricks.net`.
- The producer reaches Zerobus over the public internet; Private Link is supported for gRPC but not used here.
- Producing from Ukraine into `eastus` adds network distance: ack takes ~0.3 s against the documented ~150 ms.

## Results

**Events pushed into Delta.** First run, 60 s, whole world:

| Metric | Value |
|---|---|
| Records sent | 7,722 (6,187 positions + 1,535 static) |
| Acknowledged / failed / in flight at the end | 7,722 / 0 / 0 |
| Throughput | ~127 rec/s (~0.1% of the 100,000 rec/s per-stream quota) |
| Rows in bronze | 6,187 and 1,535, all `event_id` distinct |

**Latency**

| Step | Value |
|---|---|
| Cold start (token + stream open), once per stream | ~3.4 s |
| Producer -> durable ack | ~0.3 s |
| Producer -> queryable | 2.1 s (positions), 5.8 s (static, fewer records per commit) |
| aisstream `time_utc` -> producer | p50 ~1.4 s, p95 ~6 s |

**Idempotency.** The source sends no duplicates, so two producer instances ran on the same region at the same time for 120 s each. This is how duplicates appear in a high-availability setup.

| Producer | Sent | Acked | Replayed | Reconnects |
|---|---|---|---|---|
| `producer-b` (started 3 s earlier) | 15,920 | 15,920 | 0 | 0 |
| `producer-a` | 15,672 | 15,672 | 0 | 0 |

- Every event of `producer-a` (12,484 positions, 3,188 static) arrived with the same `event_id` in `producer-b`.
- aisstream stamps `time_utc` once per message, so the content-based key holds across independent producers.
- `producer_seq` had no gaps or repeats in any run, so all bronze duplicates come from the second producer.

| Table | Bronze rows | Distinct events | 1st MERGE inserted | 2nd MERGE inserted |
|---|---|---|---|---|
| positions | 31,348 | 18,864 | 18,864 | 0 |
| static | 7,966 | 4,778 | 4,778 | 0 |

**Normalization.** Sentinels in `raw_payload` against `NULL` in the columns (`PositionReport`):

| Field | Sentinel | `NULL` |
|---|---|---|
| `true_heading` | 10,707 × 511 | 10,712 (+5 out of range) |
| `cog` | 4,855 × ≥ 360 | 4,855 |
| `rate_of_turn` | 10,421 × −128 | 10,421 |

## Bus-based vs zero-bus

The bus-based baseline is Lab 3: Wikimedia EventStreams -> producer -> Azure Event Hub (Kafka protocol) -> Spark `readStream` -> Delta.

| | Lab 3: Event Hub (Kafka-style) | Lab 11: Zerobus |
|---|---|---|
| Path | producer -> Event Hub -> Spark job -> Delta | producer -> Zerobus -> Delta |
| Components to run | namespace and hub, producer, consumer job on a cluster, checkpoint | producer only |
| Sinks | multi-sink: many independent consumers on one topic | single-sink: the lakehouse; others read Delta |
| Buffer and replay | topic retention; consumers rewind offsets | no bus retention; reprocessing from `raw_payload` |
| If the writer side is down | events wait in the topic | the producer keeps up to 10,000 unacked records and recovers |
| Delivery and dedup | at-least-once; partition + offset | at-least-once; content `event_id` + `MERGE` |
| Ordering | per partition | per stream |
| Data in Delta | when the consumer job runs | continuously, 2–6 s |
| Schema | consumer parses raw JSON | table is the contract, validated at ingest |
| Scaling | partitions and throughput units sized upfront | serverless; more streams |
| Security | SAS connection string | OAuth service principal, table-level grants |
| Cost structure | throughput units per hour even when idle + consumer compute | volume-based, nothing when idle |

