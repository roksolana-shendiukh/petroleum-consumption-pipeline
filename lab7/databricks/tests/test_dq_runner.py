from datetime import datetime, timezone

import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

from petroleum_transformations.dq.runner import (
    RESULT_COLUMNS,
    RESULTS_SCHEMA,
    blocking_failures,
    missing_columns,
    result_status,
    run_row_checks,
    run_suite,
)

RUN_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def rule(name, column, criticality="error"):
    return {
        "name": name,
        "criticality": criticality,
        "user_metadata": {"dimension": "completeness"},
        "check": {"function": "is_not_null", "arguments": {"column": column}},
    }


def unique_rule(name, columns, criticality="error"):
    return {
        "name": name,
        "criticality": criticality,
        "user_metadata": {"dimension": "uniqueness"},
        "check": {"function": "is_unique", "arguments": {"columns": columns}},
    }


def checks():
    return [rule("id is null", "id"), rule("name is null", "name", criticality="warn")]


def engine():
    return DQEngine(WorkspaceClient())


def test_result_status_depends_on_severity():
    assert result_status(0, "error") == "PASSED"
    assert result_status(3, "error") == "FAILED"
    assert result_status(3, "warn") == "WARN"


def test_run_row_checks_counts_violations_per_rule(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b"), (3, None), (4, "d")], "id int, name string")

    results, _ = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)
    by_name = {r["test_name"]: r for r in results}

    assert by_name["id is null"]["result"] == 1
    assert by_name["id is null"]["status"] == "FAILED"
    assert by_name["name is null"]["status"] == "WARN"
    assert by_name["id is null"]["total_rows"] == 4
    assert by_name["id is null"]["dimension"] == "completeness"


def test_run_row_checks_clean_data_has_no_quarantine(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")

    results, quarantine = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)

    assert all(r["status"] == "PASSED" and r["result"] == 0 for r in results)
    assert quarantine.count() == 0


def test_quarantine_keeps_full_record_with_nulls_and_reason(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b")], "id int, name string")

    _, quarantine = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)
    row = quarantine.collect()[0]

    assert quarantine.count() == 1
    assert row["table_name"] == "silver.t"
    assert '"id":null' in row["record_json"]
    assert "id is null" in row["failed_checks"]


def test_quarantine_reasons_carry_rule_fingerprints(spark):
    df = spark.createDataFrame([(None, "b")], "id int, name string")

    _, quarantine = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)
    failed_checks = quarantine.collect()[0]["failed_checks"]

    assert "rule_fingerprint" in failed_checks
    assert "rule_set_fingerprint" in failed_checks


def test_quarantine_keeps_every_bad_row(spark):
    df = spark.createDataFrame([(None, "a")] * 5 + [(1, "b")], "id int, name string")

    results, quarantine = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)
    by_name = {r["test_name"]: r for r in results}

    assert by_name["id is null"]["result"] == 5
    assert quarantine.count() == 5


def test_is_unique_rule_flags_every_row_of_a_duplicated_key(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], "id int, name string")

    results, quarantine = run_row_checks(
        engine(), df, "t", "gold", [unique_rule("duplicate key (id)", ["id"])], "r1", RUN_TS
    )

    assert results[0]["dimension"] == "uniqueness"
    assert results[0]["result"] == 2
    assert results[0]["status"] == "FAILED"
    assert quarantine.count() == 2


def test_missing_columns_lists_typos_and_ignores_nested_fields_and_expressions(spark):
    df = spark.createDataFrame([(1, "a")], "id int, name string")
    rules = [
        rule("typo", "idd"),
        rule("nested", "name.first"),
        rule("expression", "try_element_at(tags, 1)"),
        unique_rule("composite", ["id", "nmae"]),
    ]

    assert missing_columns(df, rules) == [("typo", "idd"), ("composite", "nmae")]


def test_run_row_checks_refuses_a_check_on_a_missing_column(spark):
    df = spark.createDataFrame([(1, "a")], "id int, name string")

    with pytest.raises(ValueError, match="idd"):
        run_row_checks(engine(), df, "t", "silver", [rule("typo", "idd")], "r1", RUN_TS)


def test_run_suite_reports_a_typo_as_failed_execution(spark):
    spark.createDataFrame([(1, "a")], "id int, name string").createOrReplaceTempView("dq_typo_view")
    suite = [("silver", "dq_typo_view", [rule("typo", "idd")])]

    results_df, _ = run_suite(spark, engine(), suite, "r1", RUN_TS)
    row = results_df.collect()[0]

    assert results_df.count() == 1
    assert row["dimension"] == "execution"
    assert row["status"] == "FAILED"
    assert "idd" in row["test_name"]


def test_run_suite_records_failure_when_table_is_missing(spark):
    suite = [("silver", "no_such_catalog.no_such_schema.no_such_table", checks())]

    results_df, quarantine_df = run_suite(spark, engine(), suite, "r1", RUN_TS)
    row = results_df.collect()[0]

    assert results_df.count() == 1
    assert row["status"] == "FAILED"
    assert row["dimension"] == "execution"
    assert quarantine_df.count() == 0


def test_blocking_failures_ignores_warnings(spark):
    df = spark.createDataFrame([(1, None)], "id int, name string")
    results, _ = run_row_checks(engine(), df, "t", "silver", checks(), "r1", RUN_TS)
    rows = [tuple(r[c] for c in RESULT_COLUMNS) for r in results]

    assert blocking_failures(spark.createDataFrame(rows, RESULTS_SCHEMA)) == []