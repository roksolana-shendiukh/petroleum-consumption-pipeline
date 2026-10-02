from pyspark import pipelines as dp
from pyspark.sql import functions as F

from petroleum_transformations.quality import failed_rules

CATALOG = spark.conf.get("petroleum.catalog")
SILVER = spark.conf.get("petroleum.silver_schema")
TARGET = f"{CATALOG}.{SILVER}.petroleum_consumption"
QUARANTINE = f"{CATALOG}.{SILVER}.petroleum_consumption_quarantine"

WITHHELD_CODES = ("W", "(s)", "NA", "--", "NM")

RULES = {
    "period_is_a_date": "period_bk IS NOT NULL",
    "series_and_area_present": "series_bk IS NOT NULL AND duoarea_bk IS NOT NULL",
    "value_is_readable": "value_status != 'unparseable'",
}

dp.create_streaming_table(
    TARGET,
    comment="Cleaned petroleum consumption, one row per series, area and week",
    table_properties={"pipelines.reset.allowed": "false"},
)
dp.create_streaming_table(
    QUARANTINE,
    comment="Consumption records rejected by quality rules, with the failed rule names",
    table_properties={"pipelines.reset.allowed": "false"},
)


@dp.view(name="petroleum_consumption_typed")
def petroleum_consumption_typed():
    return (
        spark.readStream.table("petroleum_consumption_raw")
        .withColumnRenamed("series", "series_bk")
        .withColumnRenamed("duoarea", "duoarea_bk")
        .withColumnRenamed("area-name", "area_name")
        .withColumnRenamed("product", "product_bk")
        .withColumnRenamed("product-name", "product_name")
        .withColumn("period_bk", F.expr("try_cast(period as date)"))
        .withColumn("consumption_value", F.expr("try_cast(value as decimal(10,3))"))
        .withColumn(
            "value_status",
            F.when(F.col("consumption_value").isNotNull(), "ok")
            .when(F.col("value").isin(*WITHHELD_CODES), "withheld")
            .when(F.col("value").isNull(), "missing")
            .otherwise("unparseable"),
        )
        .withColumn(
            "consumption_sk",
            F.sha2(F.concat_ws("||", F.col("series_bk"), F.col("duoarea_bk"), F.col("period_bk").cast("string")), 256),
        )
        .withColumn("_source_system", F.lit("EIA_petroleum_consumption"))
        .withColumn("_sequence_key", F.struct("_file_modified_at", "_source_filename"))
    )


@dp.view(name="petroleum_consumption_cleaned")
@dp.expect_all_or_drop(RULES)
def petroleum_consumption_cleaned():
    return spark.readStream.table("petroleum_consumption_typed").select(
        "consumption_sk", "series_bk", "duoarea_bk", "area_name", "period_bk", "product_bk", "product_name",
        "units", "consumption_value", "value_status", "_source_system", "_ingested_at", "_sequence_key",
    )


@dp.append_flow(target=QUARANTINE)
def petroleum_consumption_rejected():
    return (
        spark.readStream.table("petroleum_consumption_typed")
        .withColumn("_failed_rules", failed_rules(RULES))
        .where(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
        .select(
            "series_bk", "duoarea_bk", "period", "value", "product_bk", "units",
            "_source_filename", "_failed_rules", "_quarantined_at",
        )
    )


dp.create_auto_cdc_flow(
    target=TARGET,
    source="petroleum_consumption_cleaned",
    keys=["series_bk", "duoarea_bk", "period_bk"],
    sequence_by=F.col("_sequence_key"),
    stored_as_scd_type=1,
    except_column_list=["value_status", "_sequence_key"],
)