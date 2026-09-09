# Wikipedia EventStream 

## Producer
- Async (httpx + httpx_sse) reads SSE from Wikimedia EventStreams, batches into Event Hub (Kafka SDK)
- Reconnect with exponential backoff
- Background `send_batch` calls are awaited via `asyncio.gather` before producer close (no silent failures)
- Filtering by `WIKI_FILTER` and selecting `FIELDS` is **intentionally done on the producer side**, not consumer: reduces ingress traffic and Event Hub cost before data even hits the bus
- This deviates from the "pure" bronze principle, but is justified for a streaming source backed by an open global firehose – filtering at the system boundary is a standard approach when paying to transmit unneeded data makes no sense

## Consumer
- Kafka-compatible readStream, `Trigger.AvailableNow` (bounded run, cost-aware – cluster shuts down once Event Hub is drained)
- Bronze stores `raw_payload` (raw JSON) + metadata (`eh_partition`, `eh_offset`, timestamps) without parsing
- Typing and business logic are left for silver
  
## Not implemented (justified UDF candidate)
BLP classification based on `title`/`comment` (complex pattern-matching not expressible via `regexp_extract`) – an example where a UDF is justified; belongs to the silver layer, currently not implemented.

## Known trade-offs / to fix in production
- `max_events` – an artificial cap for the lab context; in production replace with continuous/scheduled runs without a hard limit
- Parameters are passed via `dbutils.widgets` (simpler for a lab), while Databricks docs recommend a `.yml`/job JSON config for production
- Too many fine-grained Kafka timeout parameters as widgets – some (`kafka_request_timeout_ms`, `session_timeout_ms`) could be hardcoded as stable defaults

## Open question
Which parameters must remain runtime-configurable (widgets) vs which are stable enough to hardcode? 
