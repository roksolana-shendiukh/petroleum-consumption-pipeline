from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("petroleum.catalog")
SILVER = spark.conf.get("petroleum.silver_schema")
GOLD = spark.conf.get("petroleum.gold_schema")


def date_key(column):
    return F.date_format(column, "yyyyMMdd").cast("bigint")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.fct_snapshot_consumption_weekly",
    comment="Weekly consumption, one row per date, product and area",
)
def fct_snapshot_consumption_weekly():
    return spark.read.table(f"{CATALOG}.{SILVER}.petroleum_consumption").select(
        date_key("period_bk").alias("dim_date_key"),
        F.xxhash64("product_bk").alias("dim_product_key"),
        F.xxhash64("duoarea_bk").alias("dim_area_key"),
        "series_bk",
        "consumption_value",
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.fct_snapshot_prices_weekly",
    comment="Weekly prices, one row per date, product and price series",
)
def fct_snapshot_prices_weekly():
    prices = spark.read.table(f"{CATALOG}.{SILVER}.petroleum_prices")
    products = (
        spark.read.table(f"{CATALOG}.{GOLD}.dim_product")
        .where(F.col("price_product_code").isNotNull())
        .select("product_key", "price_product_code")
    )
    return prices.join(products, prices.product_bk == products.price_product_code, "inner").select(
        date_key(prices.effective_from).alias("dim_date_key"),
        products.product_key.alias("dim_product_key"),
        "series_bk",
        "price",
    )