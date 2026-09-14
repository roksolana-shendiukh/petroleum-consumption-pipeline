Silver Layer, Data Quality & Schema Evolution

## Goal

Build clean, deduplicated, analytics-ready Silver tables from the Bronze
layer, using `MERGE` for idempotent loads and Slowly Changing Dimensions
(SCD) to track history where it matters, while keeping the pipeline
resilient to schema drift and safe to re-run.

## Silver schema design

Two Silver tables, built from the corresponding Bronze sources:

![Silver schema – consumption and prices](../lab4/silver_schema.png)

- **`silver_petroleum_consumption`** (SCD Type 1 – overwrite in place):
  surrogate key `consumption_sk` (`sha2(series_bk‖duoarea_bk‖period_bk)`),
  business keys `series_bk`/`duoarea_bk`/`period_bk`, `product_bk`,
  `consumption_value`, `units`, plus metadata columns `_source_system`,
  `_ingested_at`, `_updated_at`.
- **`silver_petroleum_prices`** (SCD Type 2 – full history): surrogate
  key `price_sk` (`sha2(series_bk‖effective_from)`), `series_bk`,
  `product_name`, `price`, `units`, SCD2 validity window
  `effective_from`/`effective_to`, `is_current` flag, plus the same
  metadata columns.

**Why Type 1 for consumption but Type 2 for prices:** consumption values
for a given `(series, duoarea, period)` are corrections of the same fact –
there is no business need to keep the old, wrong value once corrected.
Prices, on the other hand, are inherently a time series where the *history
of change* is the point (e.g. "what was the price on this date"), so
losing prior values would destroy analytical value. This is the standard
criterion for choosing SCD Type 1 vs Type 2: keep history only where the
old value has value.

## Deduplication

Both pipelines deduplicate incoming Bronze data before it reaches the
MERGE step, using `row_number()` over a window partitioned by the natural
key and ordered by `ingestion_timestamp desc`, keeping only the latest
record per key. This protects against the same file being re-ingested
with overlapping rows, or multiple files covering the same period.

## MERGE pipelines

### Consumption (SCD Type 1)

Single `MERGE`: match on `consumption_sk`, `whenMatchedUpdate` only if
`consumption_value` actually changed (avoids no-op writes and unnecessary
Delta versions), `whenNotMatchedInsertAll` for new keys.

### Prices (SCD Type 2)

SCD Type 2 needs **two MERGE statements**, because a single Delta `MERGE`
cannot both update an existing row and insert a new row against the
*same* matching key in one pass:

1. **Step 1 – close the current row.** Match on
   `series_bk = series_bk AND is_current = true`; if the price changed,
   set `effective_to`, `is_current = false`.
2. **Step 2 – insert the new version.** Match on `price_sk`; insert rows
   not yet present.

**Trade-off / bug we hit and fixed:** with a rolling lookback window (see
below), a single incremental run can carry more than one new weekly price
for the same series. Matching *all* of them against the same "current"
target row in Step 1 causes
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`, since Delta
can't tell which source row should win. Fix: pre-filter Step 1's source
to only the **earliest** new version per series (the one that actually
closes the currently-open row); Step 2 still inserts *all* new versions,
since each has a unique `price_sk`.

## Incremental extraction window

The EIA API extraction pulls a rolling 30-day window
(`today - 30 days` → `today`) rather than re-pulling full history on every
run. This is cheap and self-healing for late-arriving or revised data
within the window, at the cost of the MERGE-preprocessing above to handle
multiple new rows per series per run. A shorter window (e.g. 7–14 days)
would reduce (but not eliminate) how often this preprocessing matters, at
the risk of missing revisions published with more delay.

## Schema enforcement

<!-- TODO: describe the actual test – what mismatched schema was written,
     and what error Delta raised (e.g. AnalysisException /
     DELTA_FAILED_TO_MERGE_FIELDS or a column-count/type mismatch). -->

## Schema evolution

<!-- TODO: describe the new column that was added, and whether it was
     done via `mergeSchema`/`autoMerge` on a write, or an explicit
     `ALTER TABLE ... ADD COLUMN`. -->

## Delta column mapping

<!-- TODO: describe whether `delta.columnMapping.mode = 'name'` was
     enabled, and what column was renamed or dropped as a result. -->

## Data contracts (discussion)

Schema enforcement and evolution let a Silver table survive change, but
they are reactive: a producer can still silently start sending a renamed
or dropped column, and `mergeSchema` will happily widen the table to
accommodate it without anyone being notified. A **data contract** is the
controlled alternative: a versioned, explicit schema agreement between
producer and consumer (e.g. checked in CI, or enforced at the ingestion
boundary) that fails loudly and *before* data lands, rather than silently
evolving the table underneath downstream consumers. For a single-owner
lab pipeline like this one, informal schema enforcement is proportionate;
in a multi-team production setting, a data contract would be the
recommended upgrade path.

## Reliability & re-run safety

- **Bronze → Silver is idempotent**: re-running the pipeline against the
  same Bronze data re-executes the same MERGE and produces no duplicate
  rows and no changed values (the `whenMatchedUpdate` conditions only fire
  on an actual value change).
- **Source-emptiness check runs first**, via `DESCRIBE DETAIL` on the
  Bronze table's transaction log (cheap – reads only `_delta_log`
  metadata, not the data files), so an empty or missing source fails
  fast, before paying for a full MERGE.
- Verified in practice: the Job was re-run multiple times after fixing
  bugs (`silver_prices_pipeline`: 2 attempts, `table_maintenance`: 3
  attempts) and converged to a consistent, correct state each time.

## Job scheduling

The pipeline runs as a Databricks Job on the shared all-purpose cluster
(a deliberate cost-driven exception to the job-cluster best practice, per
lab instructions), with the following task DAG:

```
api_to_volume → bronze_ingestion → ┬→ silver_consumption_pipeline ┐
                                    └→ silver_prices_pipeline      ┴→ table_maintenance
```

`environment` and `config_path` are defined once as **Job-level
parameters** and referenced by every task via
`{{job.parameters.environment}}` / `{{job.parameters.config_path}}`,
rather than duplicated per task – switching `dev` ↔ `prod` for the whole
pipeline is a single edit in Job details.

## Table maintenance: OPTIMIZE, VACUUM, and clustering

`OPTIMIZE` (file compaction) followed by `VACUUM` (removal of files no
longer referenced by the Delta log) is run on both Silver tables after
each pipeline run.

**Liquid Clustering vs ZORDER – decision: neither is applied in
production for this lab.**

- **Liquid Clustering** (`ALTER TABLE ... CLUSTER BY (...)`) is
  Databricks' current recommendation over ZORDER for new tables – it
  avoids the need to choose partition columns upfront, clusters
  incrementally, and doesn't require a full rewrite on each `OPTIMIZE`.
  It was applied only as a **learning demonstration** in a dev notebook
  (`11_table_maintenance.ipynb`), not carried into the prod pipeline.
- **ZORDER** is explicitly the older, superseded approach; Databricks
  recommends Liquid Clustering for new tables, so it was not implemented
  here – understanding the trade-off (ZORDER's static column choice and
  full-file co-location cost vs Liquid Clustering's incremental,
  cardinality-friendlier approach) was considered sufficient for this
  lab.
- **Why neither in prod:** this dataset covers only 14 series – very low
  cardinality and a small enough volume that clustering strategy has
  negligible effect on query performance. Applying either technique to
  production here would add operational complexity without a measurable
  benefit; it would matter at real production scale (many series/high
  cardinality, larger file counts).

`VACUUM` runs with the default 7-day retention in the scheduled pipeline
(safe for production – preserves Delta time-travel and avoids breaking
concurrent readers). For ad-hoc verification that VACUUM actually removes
files, a shorter retention (`RETAIN 0 HOURS` with
`retentionDurationCheck.enabled=false`) was used only in dev – never in
the scheduled prod Job.

## Data quality rules applied

- **Type-safe casting**: `try_cast(...)` on `period`/`value` fields turns
  malformed values into `null` instead of failing the whole batch, so a
  handful of bad records don't block ingestion of the rest.
- **Deduplication** by business key + latest `ingestion_timestamp`
  (above).
- **Empty-source guard** before MERGE (above).

<!-- TODO: add any explicit CHECK constraints or expectations if
     implemented, and the test that confirms they reject invalid data. -->
