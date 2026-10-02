from datetime import date
from decimal import Decimal
from petroleum_transformations.gold import (
    aggregate_consumption_monthly,
    aggregate_prices_monthly,
    build_dim_area,
    build_dim_product,
    build_fact_consumption_weekly,
    build_fact_prices_weekly,
)


def make_dim_date(spark, dates):
    rows = [(i + 1, d, d.month, d.year) for i, d in enumerate(dates)]
    return spark.createDataFrame(rows, schema=["date_key", "full_date", "month", "year"])


def test_build_dim_area_distinct_and_drops_null(spark):
    df = spark.createDataFrame(
        [("NUS", "U.S."), ("NUS", "U.S."), (None, "Unknown"), ("R10", "PADD 1")],
        schema=["duoarea_bk", "area_name"],
    )
    result = {r["area_code"]: r["area_name"] for r in build_dim_area(df).collect()}

    assert result == {"NUS": "U.S.", "R10": "PADD 1"}


def test_build_dim_product_left_join_keeps_unmatched(spark):
    mapping = spark.createDataFrame(
        [("EPD0", "Distillate", "P1"), ("EPJK", "Jet", "P2")],
        schema=["consumption_product_code", "consumption_product_name", "price_product_code"],
    )
    silver = spark.createDataFrame(
        [("EPD0", "MBBL"), ("EPD0", "MBBL")],
        schema=["product_bk", "units"],
    )
    rows = {r["consumption_product_code"]: r for r in build_dim_product(mapping, silver).collect()}

    assert len(rows) == 2
    assert rows["EPD0"]["unit_code"] == "MBBL"
    assert rows["EPJK"]["unit_code"] is None


def test_fact_consumption_weekly_inner_join_drops_unknown_area(spark):
    d = date(2026, 1, 2)
    consumption = spark.createDataFrame(
        [("S1", "NUS", d, "EPD0", Decimal("10.000")), ("S2", "ZZZ", d, "EPD0", Decimal("20.000"))],
        schema="series_bk string, duoarea_bk string, period_bk date, product_bk string, consumption_value decimal(10,3)",
    )
    dim_date = make_dim_date(spark, [d])
    dim_product = spark.createDataFrame([(1, "EPD0")], schema=["product_key", "consumption_product_code"])
    dim_area = spark.createDataFrame([(1, "NUS")], schema=["area_key", "area_code"])

    result = build_fact_consumption_weekly(consumption, dim_date, dim_product, dim_area).collect()

    assert len(result) == 1
    assert result[0]["series_bk"] == "S1"
    assert result[0]["dim_area_key"] == 1


def test_fact_prices_weekly_maps_keys(spark):
    d = date(2026, 1, 2)
    prices = spark.createDataFrame(
        [("P-S1", d, "P1", Decimal("3.50"))],
        schema="series_bk string, effective_from date, product_bk string, price decimal(10,2)",
    )
    dim_date = make_dim_date(spark, [d])
    dim_product = spark.createDataFrame([(7, "P1")], schema=["product_key", "price_product_code"])

    row = build_fact_prices_weekly(prices, dim_date, dim_product).collect()[0]

    assert row["dim_product_key"] == 7
    assert row["dim_date_key"] == 1
    assert row["price"] == Decimal("3.50")


JAN_WEEKS = [date(2026, 1, 2), date(2026, 1, 9), date(2026, 1, 16), date(2026, 1, 23), date(2026, 1, 30)]


def test_consumption_monthly_requires_four_weeks(spark):
    dim_date = make_dim_date(spark, JAN_WEEKS)
    rows = [(k, 1, 1, float(k * 10)) for k in range(1, 5)] + [(k, 2, 1, 5.0) for k in range(1, 4)]
    weekly = spark.createDataFrame(
        rows, schema=["dim_date_key", "dim_product_key", "dim_area_key", "consumption_value"]
    )

    result = aggregate_consumption_monthly(weekly, dim_date).collect()

    assert len(result) == 1
    r = result[0]
    assert r["dim_product_key"] == 1
    assert r["weeks_count"] == 4
    assert r["total_consumption"] == 100.0
    assert r["avg_weekly_consumption"] == 25.0
    assert r["min_weekly_consumption"] == 10.0
    assert r["max_weekly_consumption"] == 40.0
    assert r["dim_date_key"] == 1  


def test_prices_monthly_volatility_is_max_minus_min(spark):
    dim_date = make_dim_date(spark, JAN_WEEKS)
    weekly = spark.createDataFrame(
        [(1, 1, 3.0), (2, 1, 3.5), (3, 1, 4.0), (4, 1, 3.2)],
        schema=["dim_date_key", "dim_product_key", "price"],
    )

    result = aggregate_prices_monthly(weekly, dim_date).collect()

    assert len(result) == 1
    assert result[0]["min_price"] == 3.0
    assert result[0]["max_price"] == 4.0
    assert abs(result[0]["price_volatility"] - 1.0) < 1e-9
    assert result[0]["weeks_count"] == 4