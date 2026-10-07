# Lab 10 – External operational databases

## Part A: Lakehouse Federation

Azure SQL (AdventureWorksLT) is queried from Databricks without copying data.

## Federation vs ingestion

| | Federation | Ingestion (Delta) |
|---|---|---|
| Speed | same on small tables (1.2 s vs 1.3 s), slower on large ones | faster, data is next to the compute |
| Freshness | always current | as old as the last load |
| Load on source | every query hits it | one load |
| Security | GRANT + TLS, but firewall must allow Databricks | data is copied, the copy must be protected |
| Use for | ad hoc queries, quick checks | regular analytics, big tables |

`EXPLAIN` shows pushdown: the filter runs in Azure SQL, only matching rows return.
