from pyspark import pipelines as dp
from pyspark.sql import functions as F

from petroleum_transformations.reference import PRODUCT_PRICE_MAPPING

CATALOG = spark.conf.get("petroleum.catalog")
SILVER = spark.conf.get("petroleum.silver_schema")
GOLD = spark.conf.get("petroleum.gold_schema")

MAPPING_SCHEMA = (
    "consumption_product_code string, consumption_product_name string, price_route string, "
    "price_product_code string, is_active boolean, notes string"
)


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_product",
    comment="Products seen in consumption data, with the matching price product code when one exists",
)
def dim_product():
    mapping = spark.createDataFrame(PRODUCT_PRICE_MAPPING, MAPPING_SCHEMA).select(
        "consumption_product_code", "consumption_product_name", "price_product_code"
    )
    observed = (
        spark.read.table(f"{CATALOG}.{SILVER}.petroleum_consumption")
        .groupBy(F.col("product_bk").alias("consumption_product_code"))
        .agg(F.max("product_name").alias("observed_name"), F.max("units").alias("unit_code"))
    )
    return (
        observed.join(mapping, "consumption_product_code", "full_outer")
        .where(F.col("consumption_product_code").isNotNull())
        .select(
            F.xxhash64("consumption_product_code").alias("product_key"),
            "consumption_product_code",
            "price_product_code",
            F.coalesce("consumption_product_name", "observed_name").alias("product_name"),
            "unit_code",
        )
    )