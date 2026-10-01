# Lab 7 – Unit Tests & Data Quality

## Part A: Unit tests

Transformation logic from the pipeline notebooks was extracted into importable modules (`src/petroleum_transformations/`: config, eia_client, bronze, silver, gold) and covered with 38 `pytest` unit tests (`tests/`).

**Characteristics of the tests:**
- **Isolated:** each test targets one pure `DataFrame -> DataFrame` function on a few hand-made rows; no real tables are read or written.
- **Independent:** tests share no state and do not depend on dev data.
- **Fast feedback:** HTTP calls are mocked and retry waits are disabled.
- **Real Spark, local code:** tests run locally, Spark executes on the cluster via Databricks Connect.
- **Headless:** the same suite runs as a Databricks bundle job; any failure fails the job (CI gate).

`MERGE` and table I/O are integration level and not unit-tested.

## Part B: Data quality

DQX checks run on the real tables of all three layers and write the results to tables. Existing tables are not changed.

**Characteristics:**
- **Declarative:** rules are YAML files, one per table; thresholds come from the config.
- **Validated:** rules are checked for errors and duplicates before they run.
- **Versioned:** a job deploys the rules to the Delta table `dq_checks`.
- **Measured:** `dq_test_results` has one row per check: dimension, rows checked, violations, status.
- **Traceable:** `dq_quarantine` keeps every bad record as JSON with the failed rule.
- **Cross-table:** referential integrity, reconciliation between layers and freshness are separate checks.

The suite found real problems, for example 6,446 fact rows with outdated product keys and 88 prices without a date.
The lab 5 Lakeflow pipeline already drops bad rows and writes them to quarantine tables. Each rule set is defined once (DRY) and used for both.
