from datetime import date, datetime
from decimal import Decimal

from petroleum_transformations.silver import (
    add_validity_window,
    build_silver_prices,
    clean_prices,
    earliest_per_series,
)

BRONZE_COLS = [
    "series", "product", "product-name", "period", "value",
    "units", "source_filename", "ingestion_timestamp",
]
TS = datetime(2026, 1, 1)


def make_bronze(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_COLS)


def row(series, period, value, ts=TS):
    return (series, "P1", "Diesel", period, value, "$/GAL", "f.json", ts)


def test_clean_prices_renames_and_casts(spark):
    r = clean_prices(make_bronze(spark, [row("S1", "2026-01-02", "3.5")])).collect()[0]

    assert r["series_bk"] == "S1"
    assert r["product_name"] == "Diesel"
    assert r["effective_from"] == date(2026, 1, 2)
    assert r["price"] == Decimal("3.500")


def test_clean_prices_drops_null_zero_and_negative(spark):
    df = make_bronze(spark, [
        row("S1", "2026-01-02", "3.5"),
        row("S1", "2026-01-09", "abc"),
        row("S1", "2026-01-16", "0"),
        row("S1", "2026-01-23", "-1"),
        row("S1", "2026-01-30", None),
    ])
    result = clean_prices(df).collect()

    assert len(result) == 1
    assert result[0]["effective_from"] == date(2026, 1, 2)


def test_clean_prices_invalid_date_gives_null_effective_from(spark):
    r = clean_prices(make_bronze(spark, [row("S1", "not-a-date", "3.5")])).collect()[0]

    assert r["effective_from"] is None


def test_validity_window_chains_versions_and_marks_current(spark):
    df = spark.createDataFrame(
        [("S1", date(2026, 1, 9)), ("S1", date(2026, 1, 2)), ("S1", date(2026, 1, 16))],
        schema=["series_bk", "effective_from"],
    )
    rows = {r["effective_from"]: r for r in add_validity_window(df).collect()}

    assert rows[date(2026, 1, 2)]["effective_to"] == date(2026, 1, 9)
    assert rows[date(2026, 1, 9)]["effective_to"] == date(2026, 1, 16)
    assert rows[date(2026, 1, 16)]["effective_to"] is None
    assert [r["is_current"] for r in sorted(rows.values(), key=lambda r: r["effective_from"])] == [False, False, True]


def test_validity_window_is_independent_per_series(spark):
    df = spark.createDataFrame(
        [("S1", date(2026, 1, 2)), ("S2", date(2026, 1, 9))],
        schema=["series_bk", "effective_from"],
    )
    result = add_validity_window(df).collect()

    assert all(r["is_current"] for r in result)


def test_earliest_per_series_picks_first_row(spark):
    df = spark.createDataFrame(
        [
            ("S1", date(2026, 1, 9), Decimal("4.0")),
            ("S1", date(2026, 1, 2), Decimal("3.0")),
            ("S2", date(2026, 1, 16), Decimal("5.0")),
        ],
        schema="series_bk string, effective_from date, price decimal(10,3)",
    )
    rows = {r["series_bk"]: r for r in earliest_per_series(df).collect()}

    assert rows["S1"]["effective_from"] == date(2026, 1, 2)
    assert rows["S1"]["price"] == Decimal("3.000")
    assert rows["S2"]["effective_from"] == date(2026, 1, 16)


def test_build_silver_prices_end_to_end(spark):
    old, new = datetime(2026, 1, 1), datetime(2026, 1, 20)
    df = make_bronze(spark, [
        row("S1", "2026-01-02", "3.0", old),
        row("S1", "2026-01-02", "3.2", new),   
        row("S1", "2026-01-09", "3.5", old),
        row("S1", "2026-01-16", "0", old),     
    ])
    result = build_silver_prices(df)
    rows = sorted(result.collect(), key=lambda r: r["effective_from"])

    assert len(rows) == 2
    assert rows[0]["price"] == Decimal("3.200")
    assert rows[0]["effective_to"] == date(2026, 1, 9)
    assert rows[0]["is_current"] is False
    assert rows[1]["is_current"] is True
    assert rows[0]["price_sk"] != rows[1]["price_sk"]
    assert rows[0]["_source_system"] == "EIA_petroleum_prices"
    assert rows[0]["_updated_at"] is None