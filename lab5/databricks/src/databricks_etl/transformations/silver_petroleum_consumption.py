from pyspark import pipelines as dp
from pyspark.sql.functions import expr, lit, col, struct

dp.create_streaming_table("petroleum_consumption_ldp_silver")
dp.create_streaming_table(
    "petroleum_consumption_quarantine",
    table_properties={"pipelines.reset.allowed": "false"}
)

CONSUMPTION_RULES = {
    "valid_period": "period_bk IS NOT NULL",
    "valid_series": "series_bk IS NOT NULL AND duoarea_bk IS NOT NULL",
    "valid_consumption_value": (
        "consumption_value IS NULL OR consumption_value >= 0"
    ),
}


@dp.view(
    name="petroleum_consumption_raw_typed",
    comment="Typed/renamed consumption stream shared by cleaned and quarantine flows"
)
def petroleum_consumption_raw_typed():
    return (
        spark.readStream.table("petroleum_consumption_raw_ldp_bronze")
            .withColumnRenamed("series", "series_bk")
            .withColumnRenamed("duoarea", "duoarea_bk")
            .withColumnRenamed("product", "product_bk")
            .withColumn("period_bk", expr("try_cast(period as date)"))
            .withColumn("consumption_value", expr("try_cast(value as decimal(10,3))"))
            .withColumn("_sequence_key", struct(col("period_bk"), col("_ingested_at")))
    )


@dp.view(
    name="petroleum_consumption_cleaned",
    comment="Consumption records passing all quality expectations"
)
@dp.expect_all_or_drop(CONSUMPTION_RULES)
def petroleum_consumption_cleaned():
    return spark.readStream.table("petroleum_consumption_raw_typed").select(
        "series_bk", "duoarea_bk", "period_bk", "product_bk",
        "units", "consumption_value", "value", "_ingested_at", "_sequence_key"
    )


@dp.append_flow(target="petroleum_consumption_quarantine")
def petroleum_consumption_rejected():
    failed_condition = " OR ".join(f"NOT ({rule})" for rule in CONSUMPTION_RULES.values())
    return (
        spark.readStream.table("petroleum_consumption_raw_typed")
            .where(failed_condition)
            .withColumn("_quarantined_at", expr("current_timestamp()"))
            .withColumn("_pipeline", lit("petroleum_consumption"))
    )


dp.create_auto_cdc_flow(
    target="petroleum_consumption_ldp_silver",
    source="petroleum_consumption_cleaned",
    keys=["series_bk", "duoarea_bk", "period_bk"],
    sequence_by=col("_sequence_key"),
    stored_as_scd_type=1,
    except_column_list=["value", "_sequence_key"]
)