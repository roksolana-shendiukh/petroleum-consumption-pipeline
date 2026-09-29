from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col
from pyspark.sql.functions import avg, count
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum


def build_dim_product(mapping_df: DataFrame, consumption_silver_df: DataFrame) -> DataFrame:
    """Product dimension: price mapping enriched with unit code from consumption."""
    mapping = mapping_df.select(
        col("consumption_product_code"),
        col("consumption_product_name").alias("product_name"),
        col("price_product_code"),
    )
    units = (
        consumption_silver_df
        .select(col("product_bk").alias("consumption_product_code"), col("units").alias("unit_code"))
        .distinct()
    )
    return mapping.join(units, on="consumption_product_code", how="left").select(
        "consumption_product_code", "price_product_code", "product_name", "unit_code"
    )


def build_dim_area(consumption_silver_df: DataFrame) -> DataFrame:
    """Area dimension: distinct non-null area codes with names."""
    return (
        consumption_silver_df
        .select(col("duoarea_bk").alias("area_code"), col("area_name"))
        .distinct()
        .filter(col("area_code").isNotNull())
    )


def build_fact_consumption_weekly(
    consumption_df: DataFrame, dim_date_df: DataFrame, dim_product_df: DataFrame, dim_area_df: DataFrame
) -> DataFrame:
    dates = dim_date_df.select("date_key", "full_date")
    products = dim_product_df.select("product_key", "consumption_product_code")
    areas = dim_area_df.select("area_key", "area_code")

    joined = (
        consumption_df
        .join(broadcast(dates), consumption_df.period_bk == dates.full_date, "inner")
        .join(broadcast(products), consumption_df.product_bk == products.consumption_product_code, "inner")
        .join(broadcast(areas), consumption_df.duoarea_bk == areas.area_code, "inner")
    )
    return joined.select(
        col("date_key").alias("dim_date_key"),
        col("product_key").alias("dim_product_key"),
        col("area_key").alias("dim_area_key"),
        col("series_bk"),
        col("consumption_value"),
    )


def build_fact_prices_weekly(prices_df: DataFrame, dim_date_df: DataFrame, dim_product_df: DataFrame) -> DataFrame:
    dates = dim_date_df.select("date_key", "full_date")
    products = dim_product_df.select("product_key", "price_product_code")

    joined = (
        prices_df
        .join(broadcast(dates), prices_df.effective_from == dates.full_date, "inner")
        .join(broadcast(products), prices_df.product_bk == products.price_product_code, "inner")
    )
    return joined.select(
        col("date_key").alias("dim_date_key"),
        col("product_key").alias("dim_product_key"),
        col("series_bk"),
        col("price"),
    )


def _weekly_with_dates(weekly_df: DataFrame, dim_date_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    dates = dim_date_df.select(col("date_key").alias("dim_date_key"), "full_date", "month", "year")
    return weekly_df.join(dates, "dim_date_key"), dates


def aggregate_consumption_monthly(weekly_df: DataFrame, dim_date_df: DataFrame, min_weeks: int = 4) -> DataFrame:
    joined, dates = _weekly_with_dates(weekly_df, dim_date_df)
    agg = (
        joined
        .groupBy("dim_product_key", "dim_area_key", "year", "month")
        .agg(
            spark_min("full_date").alias("month_start_date"),
            spark_sum("consumption_value").alias("total_consumption"),
            avg("consumption_value").alias("avg_weekly_consumption"),
            spark_min("consumption_value").alias("min_weekly_consumption"),
            spark_max("consumption_value").alias("max_weekly_consumption"),
            count("consumption_value").alias("weeks_count"),
        )
        .filter(col("weeks_count") >= min_weeks)
    )
    return agg.join(
        dates.select(col("dim_date_key"), col("full_date")),
        agg.month_start_date == dates.full_date,
        "inner",
    ).select(
        "dim_date_key", "dim_product_key", "dim_area_key",
        "total_consumption", "avg_weekly_consumption",
        "min_weekly_consumption", "max_weekly_consumption", "weeks_count",
    )


def aggregate_prices_monthly(weekly_df: DataFrame, dim_date_df: DataFrame, min_weeks: int = 4) -> DataFrame:
    joined, dates = _weekly_with_dates(weekly_df, dim_date_df)
    agg = (
        joined
        .groupBy("dim_product_key", "year", "month")
        .agg(
            spark_min("full_date").alias("month_start_date"),
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            count("price").alias("weeks_count"),
        )
        .filter(col("weeks_count") >= min_weeks)
        .withColumn("price_volatility", col("max_price") - col("min_price"))
    )
    return agg.join(
        dates.select(col("dim_date_key"), col("full_date")),
        agg.month_start_date == dates.full_date,
        "inner",
    ).select(
        "dim_date_key", "dim_product_key",
        "avg_price", "min_price", "max_price", "price_volatility", "weeks_count",
    )

