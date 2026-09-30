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

![Unit tests passed](../../screenshots/lab7_unit_tests_passed.png)

## Part B: Data quality
