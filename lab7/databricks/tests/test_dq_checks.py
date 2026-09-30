from datetime import date, datetime, timedelta, timezone

from petroleum_transformations.dq.checks import (
    RunContext,
    check_age,
    check_count_reconciliation,
    check_coverage,
    check_reference,
    check_sum_reconciliation,
    check_uniqueness,
)

CTX = RunContext("r1", datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_uniqueness_counts_extra_rows_and_quarantines_all_duplicates(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a"), (1, "a"), (2, "b")], "id int, name string")

    row, quarantine = check_uniqueness(CTX, "gold", "t", df, ["id"], "error")

    assert row["result"] == 2
    assert row["total_rows"] == 4
    assert row["status"] == "FAILED"
    assert row["dimension"] == "uniqueness"
    assert quarantine.count() == 3


def test_uniqueness_clean_data_has_no_quarantine(spark):
    df = spark.createDataFrame([(1,), (2,)], "id int")

    row, quarantine = check_uniqueness(CTX, "gold", "t", df, ["id"], "error")

    assert row["result"] == 0
    assert row["status"] == "PASSED"
    assert quarantine is None


def test_uniqueness_ignores_null_keys_and_respects_warn(spark):
    df = spark.createDataFrame([(None,), (None,), (1,), (1,)], "id int")

    row, _ = check_uniqueness(CTX, "gold", "t", df, ["id"], "warn")

    assert row["total_rows"] == 2
    assert row["result"] == 1
    assert row["status"] == "WARN"


def test_reference_finds_orphans_and_quarantines_them(spark):
    child = spark.createDataFrame([(1,), (2,), (3,), (None,)], "date_key int")
    parent = spark.createDataFrame([(1,), (2,)], "key int")

    row, quarantine = check_reference(CTX, "silver", "t", child, "date_key", parent, "key", "dim", "error")

    assert row["total_rows"] == 3
    assert row["result"] == 1
    assert row["status"] == "FAILED"
    assert row["dimension"] == "consistency"
    assert quarantine.count() == 1
    assert '"date_key":3' in quarantine.collect()[0]["record_json"]


def test_reference_all_matched(spark):
    child = spark.createDataFrame([(1,), (2,)], "k int")
    parent = spark.createDataFrame([(1,), (2,), (3,)], "key int")

    row, quarantine = check_reference(CTX, "silver", "t", child, "k", parent, "key", "dim", "error")

    assert row["result"] == 0
    assert quarantine is None


def test_coverage_reports_days_beyond_allowed_lag(spark):
    parent = spark.createDataFrame([(date(2026, 9, 7),)], "d date")
    child = spark.createDataFrame([(date(2026, 9, 21),), (date(2026, 9, 1),)], "d date")

    row, _ = check_coverage(CTX, "gold", "dim_date", parent, "d", child, "d", "silver", 0, "error")
    tolerated, _ = check_coverage(CTX, "gold", "dim_date", parent, "d", child, "d", "silver", 14, "error")

    assert row["result"] == 14
    assert row["status"] == "FAILED"
    assert row["dimension"] == "timeliness"
    assert tolerated["status"] == "PASSED"


def test_age_flags_stale_data_and_accepts_fresh_data(spark):
    stale = spark.createDataFrame([(date.today() - timedelta(days=40),)], "d date")
    fresh = spark.createDataFrame([(date.today(),)], "d date")

    old_row, _ = check_age(CTX, "silver", "t", stale, "d", 14, "warn")
    new_row, _ = check_age(CTX, "silver", "t", fresh, "d", 14, "warn")

    assert old_row["result"] == 26
    assert old_row["status"] == "WARN"
    assert new_row["status"] == "PASSED"


def test_count_reconciliation_uses_tolerance(spark):
    source = spark.range(1000)

    same, _ = check_count_reconciliation(CTX, "gold", "t", "n", source, spark.range(1000), 0.5)
    small, _ = check_count_reconciliation(CTX, "gold", "t", "n", source, spark.range(996), 0.5)
    large, _ = check_count_reconciliation(CTX, "gold", "t", "n", source, spark.range(900), 0.5)

    assert same["status"] == "PASSED"
    assert small["status"] == "WARN"
    assert small["result"] == 4
    assert large["status"] == "FAILED"
    assert large["result"] == 100


def test_count_reconciliation_flags_surplus_in_target(spark):
    row, _ = check_count_reconciliation(CTX, "gold", "t", "n", spark.range(1000), spark.range(1200), 0.5)

    assert row["result"] == 200
    assert row["status"] == "FAILED"


def test_sum_reconciliation_detects_value_loss(spark):
    source = spark.createDataFrame([(100.0,), (50.0,)], "v double")
    target = spark.createDataFrame([(100.0,)], "v double")

    row, _ = check_sum_reconciliation(CTX, "gold", "t", "sum", source, "v", target, "v", 0.5)

    assert row["result"] == 50
    assert row["total_rows"] == 2
    assert row["status"] == "FAILED"