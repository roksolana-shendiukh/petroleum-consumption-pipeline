from datetime import datetime, timezone

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

from petroleum_transformations.dq.rules import make_rule
from petroleum_transformations.dq.runner import (
    blocking_failures,
    result_status,
    run_row_checks,
    run_suite,
)

RUN_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def rules():
    return [
        make_rule("id is null", "completeness", check_funcs.is_not_null, "id"),
        make_rule("name is null", "completeness", check_funcs.is_not_null, "name", criticality="warn"),
    ]


def engine():
    return DQEngine(WorkspaceClient())


def test_result_status_depends_on_severity():
    assert result_status(0, "error") == "PASSED"
    assert result_status(3, "error") == "FAILED"
    assert result_status(3, "warn") == "WARN"


def test_run_row_checks_counts_violations_per_rule(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b"), (3, None), (4, "d")], "id int, name string")

    results, _ = run_row_checks(engine(), df, "t", "silver", rules(), "r1", RUN_TS)
    by_name = {r["test_name"]: r for r in results}

    assert by_name["id is null"]["result"] == 1
    assert by_name["id is null"]["status"] == "FAILED"
    assert by_name["name is null"]["status"] == "WARN"
    assert by_name["id is null"]["total_rows"] == 4
    assert by_name["id is null"]["dimension"] == "completeness"


def test_run_row_checks_clean_data_has_no_quarantine(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")

    results, quarantine = run_row_checks(engine(), df, "t", "silver", rules(), "r1", RUN_TS)

    assert all(r["status"] == "PASSED" and r["result"] == 0 for r in results)
    assert quarantine.count() == 0


def test_quarantine_keeps_full_record_with_nulls_and_reason(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b")], "id int, name string")

    _, quarantine = run_row_checks(engine(), df, "t", "silver", rules(), "r1", RUN_TS)
    row = quarantine.collect()[0]

    assert quarantine.count() == 1
    assert row["table_name"] == "silver.t"
    assert '"id":null' in row["record_json"]
    assert "id is null" in row["failed_checks"]


def test_run_suite_records_failure_when_table_is_missing(spark):
    suite = [("silver", "no_such_catalog.no_such_schema.no_such_table", rules())]

    results_df, quarantine_df = run_suite(spark, engine(), suite, "r1", RUN_TS)
    row = results_df.collect()[0]

    assert results_df.count() == 1
    assert row["status"] == "FAILED"
    assert row["dimension"] == "execution"
    assert quarantine_df.count() == 0


def test_blocking_failures_ignores_warnings(spark):
    df = spark.createDataFrame([(1, None)], "id int, name string")
    results, _ = run_row_checks(engine(), df, "t", "silver", rules(), "r1", RUN_TS)
    rows = [tuple(r[c] for c in r) for r in results]
    results_df = spark.createDataFrame(
        rows,
        "run_id string, run_ts timestamp, layer string, table_name string, dimension string, "
        "test_name string, total_rows bigint, result bigint, status string, severity string",
    )

    assert blocking_failures(results_df) == []