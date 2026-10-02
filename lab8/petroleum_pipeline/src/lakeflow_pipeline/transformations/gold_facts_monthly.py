from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("petroleum.catalog")
GOLD = spark.conf.get("petroleum.gold_schema")
MIN_WEEKS = int(spark.conf.get("petroleum.min_weeks_per_month"))


def month_start(column):
    return F.trunc(F.to_date(F.col(column).cast("string"), "yyyyMMdd"), "month")


def month_key(column):
    return F.date_format(column, "yyyyMMdd").cast("bigint")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.fct_snapshot_consumption_monthly",
    comment="Monthly consumption per product and area; months with too few weeks are left out",
)
def fct_snapshot_consumption_monthly():
    weekly = spark.read.table(f"{CATALOG}.{GOLD}.fct_snapshot_consumption_weekly")
    return (
        weekly.withColumn("month_start", month_start("dim_date_key"))
        .groupBy("dim_product_key", "dim_area_key", "month_start")
        .agg(
            F.sum("consumption_value").alias("total_consumption"),
            F.avg("consumption_value").alias("avg_weekly_consumption"),
            F.min("consumption_value").alias("min_weekly_consumption"),
            F.max("consumption_value").alias("max_weekly_consumption"),
            F.countDistinct(F.when(F.col("consumption_value").isNotNull(), F.col("dim_date_key"))).alias("weeks_count"),
        )
        .where(F.col("weeks_count") >= MIN_WEEKS)
        .select(
            month_key("month_start").alias("dim_date_key"),
            "dim_product_key", "dim_area_key",
            "total_consumption", "avg_weekly_consumption",
            "min_weekly_consumption", "max_weekly_consumption", "weeks_count",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.fct_snapshot_prices_monthly",
    comment="Monthly prices per product and price series; weeks_count counts distinct weeks",
)
def fct_snapshot_prices_monthly():
    weekly = spark.read.table(f"{CATALOG}.{GOLD}.fct_snapshot_prices_weekly")
    return (
        weekly.withColumn("month_start", month_start("dim_date_key"))
        .groupBy("dim_product_key", "series_bk", "month_start")
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.countDistinct("dim_date_key").alias("weeks_count"),
        )
        .where(F.col("weeks_count") >= MIN_WEEKS)
        .withColumn("price_volatility", F.col("max_price") - F.col("min_price"))
        .select(
            month_key("month_start").alias("dim_date_key"),
            "dim_product_key", "series_bk",
            "avg_price", "min_price", "max_price", "price_volatility", "weeks_count",
        )
    )