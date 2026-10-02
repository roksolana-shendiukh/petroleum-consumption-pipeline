from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("petroleum.catalog")
SILVER = spark.conf.get("petroleum.silver_schema")
GOLD = spark.conf.get("petroleum.gold_schema")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_area",
    comment="Areas seen in consumption data",
)
def dim_area():
    return (
        spark.read.table(f"{CATALOG}.{SILVER}.petroleum_consumption")
        .where(F.col("duoarea_bk").isNotNull())
        .groupBy(F.col("duoarea_bk").alias("area_code"))
        .agg(F.max("area_name").alias("area_name"))
        .select(F.xxhash64("area_code").alias("area_key"), "area_code", "area_name")
    )