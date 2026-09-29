from datetime import date, datetime
from decimal import Decimal

import pytest

from petroleum_transformations.silver import (
    add_surrogate_key,
    build_silver_consumption,
    clean_consumption,
    dedupe_latest,
)

BRONZE_COLS = [
    "series", "duoarea", "area-name", "product", "period", "value",
    "units", "source_filename", "ingestion_timestamp",
]


def make_bronze(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_COLS)


def test_clean_consumption_renames_and_casts(spark):
    ts = datetime(2026, 1, 1)
    df = make_bronze(spark, [("S1", "NUS", "U.S.", "EPD0", "2026-01-02", "123.5", "MBBL", "f.json", ts)])
    row = clean_consumption(df).collect()[0]

    assert row["series_bk"] == "S1"
    assert row["area_name"] == "U.S."
    assert row["period_bk"] == date(2026, 1, 2)
    assert row["consumption_value"] == Decimal("123.500")


def test_clean_consumption_invalid_value_becomes_null(spark):
    ts = datetime(2026, 1, 1)
    df = make_bronze(spark, [("S1", "NUS", "U.S.", "EPD0", "2026-01-02", "abc", "MBBL", "f.json", ts)])
    row = clean_consumption(df).collect()[0]

    assert row["consumption_value"] is None


def test_clean_consumption_invalid_date_becomes_null(spark):
    ts = datetime(2026, 1, 1)
    df = make_bronze(spark, [("S1", "NUS", "U.S.", "EPD0", "not-a-date", "1", "MBBL", "f.json", ts)])
    row = clean_consumption(df).collect()[0]

    assert row["period_bk"] is None


def test_dedupe_latest_keeps_newest_ingestion(spark):
    df = spark.createDataFrame(
        [
            ("S1", "NUS", 10, datetime(2026, 1, 1)),
            ("S1", "NUS", 20, datetime(2026, 1, 2)),
            ("S2", "NUS", 30, datetime(2026, 1, 1)),
        ],
        schema=["series_bk", "duoarea_bk", "val", "ingestion_timestamp"],
    )
    result = dedupe_latest(df, ["series_bk", "duoarea_bk"], "ingestion_timestamp")
    values = {r["series_bk"]: r["val"] for r in result.collect()}

    assert result.count() == 2
    assert values == {"S1": 20, "S2": 30}


def test_surrogate_key_is_deterministic_and_distinct(spark):
    df = spark.createDataFrame(
        [("A", "1"), ("A", "1"), ("A", "2")], schema=["k1", "k2"]
    )
    rows = add_surrogate_key(df, ["k1", "k2"], "sk").collect()

    assert rows[0]["sk"] == rows[1]["sk"]
    assert rows[0]["sk"] != rows[2]["sk"]
    assert len(rows[0]["sk"]) == 64


def test_build_silver_consumption_end_to_end(spark):
    old, new = datetime(2026, 1, 1), datetime(2026, 1, 2)
    df = make_bronze(spark, [
        ("S1", "NUS", "U.S.", "EPD0", "2026-01-02", "100", "MBBL", "a.json", old),
        ("S1", "NUS", "U.S.", "EPD0", "2026-01-02", "150", "MBBL", "b.json", new),
    ])
    result = build_silver_consumption(df)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["consumption_value"] == Decimal("150.000")
    assert rows[0]["_source_system"] == "EIA_petroleum_consumption"
    assert rows[0]["_updated_at"] is None
    assert "consumption_sk" in result.columns

