from pyspark import pipelines as dp
from pyspark.sql.functions import expr, struct, lit, col, when

dp.create_streaming_table(
    "petroleum_prices_ldp_silver",
    comment="SCD Type 2 history of petroleum prices by series",
    table_properties={
        "pipelines.reset.allowed": "false"
    }
)
dp.create_streaming_table(
    "petroleum_prices_quarantine",
    comment="Price records rejected by quality expectations",
    table_properties={"pipelines.reset.allowed": "false"}
)

WITHHELD_CODES = ("W", "(s)", "NA", "--", "NM")

PRICES_RULES = {
    "valid_effective_from": "effective_from IS NOT NULL",
    "valid_series": "series_bk IS NOT NULL",
    "valid_price": "price_status != 'unparseable'",
}


@dp.view(
    name="petroleum_prices_raw_typed",
    comment="Typed/renamed prices stream shared by cleaned and quarantine flows"
)
def petroleum_prices_raw_typed():
    return (
        spark.readStream.table("petroleum_prices_raw_ldp_bronze")
            .withColumnRenamed("series", "series_bk")
            .withColumnRenamed("product-name", "product_name")
            .withColumn("effective_from", expr("try_cast(period as date)"))
            .withColumn("price", expr("try_cast(value as decimal(10,3))"))
            .withColumn(
                "price_status",
                when(col("price").isNotNull(), "ok")
                .when(col("value").isin(*WITHHELD_CODES), "withheld")
                .when(col("value").isNull(), "missing")
                .otherwise("unparseable")
            )
            .select(
                "series_bk", "product_name", "units", "price", "price_status",
                "effective_from", "value", "_ingested_at"
            )
            .withColumn("_sequence_key", struct(col("effective_from"), col("_ingested_at")))
    )


@dp.view(
    name="petroleum_prices_cleaned",
    comment="Price records passing all quality expectations"
)
@dp.expect_all_or_drop(PRICES_RULES)
def petroleum_prices_cleaned():
    return spark.readStream.table("petroleum_prices_raw_typed").select(
        "series_bk", "product_name", "units", "price", "price_status",
        "effective_from", "_ingested_at", "_sequence_key"
    )


@dp.append_flow(target="petroleum_prices_quarantine")
def petroleum_prices_rejected():
    failed_condition = " OR ".join(f"NOT ({rule})" for rule in PRICES_RULES.values())
    return (
        spark.readStream.table("petroleum_prices_raw_typed")
            .where(failed_condition)
            .withColumn("_quarantined_at", expr("current_timestamp()"))
            .withColumn("_pipeline", lit("petroleum_prices"))
    )


dp.create_auto_cdc_flow(
    target="petroleum_prices_ldp_silver",
    source="petroleum_prices_cleaned",
    keys=["series_bk"],
    sequence_by=col("_sequence_key"),
    stored_as_scd_type=2,
    except_column_list=["_sequence_key"]
)