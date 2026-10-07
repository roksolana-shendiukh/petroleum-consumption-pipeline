# Lab 10 – External operational databases

## Part A: Lakehouse Federation

Azure SQL (AdventureWorksLT) is queried from Databricks without copying data.

### Federation vs ingestion

| | Federation | Ingestion (Delta) |
|---|---|---|
| Speed | same on small tables (1.2 s vs 1.3 s), slower on large ones | faster, data is next to the compute |
| Freshness | always current | as old as the last load |
| Load on source | every query hits it | one load |
| Security | GRANT + TLS, but firewall must allow Databricks | data is copied, the copy must be protected |
| Use for | ad hoc queries, quick checks | regular analytics, big tables |

`EXPLAIN` shows pushdown: the filter runs in Azure SQL, only matching rows return.


## Part B: Change Data Capture

Delta Change Data Feed on `product_delta` feeds an SCD Type 2 table `dim_product_scd2`.

### Technical notes

- Changes come from a Delta table, not from Azure SQL: Change Data Feed reads the Delta
  transaction log. SQL Server CDC would read the database log, but it needs CDC enabled
  on the server and a higher Azure SQL tier than Basic.
- `table_changes(table, version)` reads only new versions, so each run is incremental.
- `update_preimage` rows are skipped, only the latest change per key is applied.
- Delete closes the record (`is_current = false`), the history stays.
- The target should be clustered by `ProductID` (liquid clustering), so MERGE reads only
  the files with the changed keys. No visible effect on this small table.
- Known limit: the same version range must not be merged twice (duplicates). A checkpoint with the last processed version solves it.

### Batch vs CDC

| | Batch (full reload) | CDC |
|---|---|---|
| Data moved | whole table every time | only changed rows |
| Deletes | invisible unless compared | captured as events |
| History | lost on overwrite | kept (SCD Type 2) |
| Cost | grows with table size | grows with number of changes |

Late-arriving data: `valid_from` is the commit time, not the business time, so a record
that arrives late is not inserted into the middle of the history.
