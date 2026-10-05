from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from petroleum_transformations.quality import failed_rules

CATALOG = spark.conf.get("petroleum.catalog")
SILVER = spark.conf.get("petroleum.silver_schema")
DEDUPED = f"{CATALOG}.{SILVER}.petroleum_prices_deduped"
QUARANTINE = f"{CATALOG}.{SILVER}.petroleum_prices_quarantine"
FINAL = f"{CATALOG}.{SILVER}.petroleum_prices"

RULES = {
    "effective_from_is_a_date": "effective_from IS NOT NULL",
    "series_present": "series_bk IS NOT NULL",
    "price_is_positive": "price IS NOT NULL AND price > 0",
}

dp.create_streaming_table(
    DEDUPED,
    comment="Latest price per series and week, before validity windows are added",
    table_properties={"pipelines.reset.allowed": "false"},
)
dp.create_streaming_table(
    QUARANTINE,
    comment="Price records rejected by quality rules, with the failed rule names",
    table_properties={"pipelines.reset.allowed": "false"},
)


@dp.view(name="petroleum_prices_typed")
def petroleum_prices_typed():
    return (
        spark.readStream.table("petroleum_prices_raw")
        .withColumnRenamed("series", "series_bk")
        .withColumnRenamed("product", "product_bk")
        .withColumnRenamed("product-name", "product_name")
        .withColumn("effective_from", F.expr("try_cast(period as date)"))
        .withColumn("price", F.expr("try_cast(value as decimal(10,3))"))
        .withColumn("_sequence_key", F.struct("_file_modified_at", "_source_filename"))
    )


@dp.view(name="petroleum_prices_cleaned")
@dp.expect_all_or_drop(RULES)
def petroleum_prices_cleaned():
    return spark.readStream.table("petroleum_prices_typed").select(
        "series_bk", "product_bk", "product_name", "units", "price", "effective_from",
        "_ingested_at", "_sequence_key",
    )


@dp.append_flow(target=QUARANTINE)
def petroleum_prices_rejected():
    return (
        spark.readStream.table("petroleum_prices_typed")
        .withColumn("_failed_rules", failed_rules(RULES))
        .where(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
        .select(
            "series_bk", "product_bk", "period", "value", "units",
            "_source_filename", "_failed_rules", "_quarantined_at",
        )
    )


dp.create_auto_cdc_flow(
    target=DEDUPED,
    source="petroleum_prices_cleaned",
    keys=["series_bk", "effective_from"],
    sequence_by=F.col("_sequence_key"),
    stored_as_scd_type=1,
    except_column_list=["_sequence_key"],
)


@dp.materialized_view(
    name=FINAL,
    comment="Prices with validity windows: effective_to is the next week of the same series",
)
def petroleum_prices():
    window = Window.partitionBy("series_bk").orderBy("effective_from")
    return (
        spark.read.table(DEDUPED)
        .withColumn("price_sk", F.sha2(F.concat_ws("||", F.col("series_bk"), F.col("effective_from").cast("string")), 256))
        .withColumn("effective_to", F.lead("effective_from", 1).over(window))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn("_source_system", F.lit("EIA_petroleum_prices"))
        .select(
            "price_sk", "series_bk", "product_bk", "product_name", "units", "price",
            "effective_from", "effective_to", "is_current", "_source_system", "_ingested_at",
        )
    )