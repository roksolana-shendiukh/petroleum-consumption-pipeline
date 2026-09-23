from pyspark import pipelines as dp
from pyspark.sql.functions import expr

dp.create_streaming_table("petroleum_consumption_ldp_silver")


@dp.view(name="petroleum_consumption_cleaned")
@dp.expect_or_drop("valid_period", "period_bk IS NOT NULL")
@dp.expect_or_drop("valid_series", "series_bk IS NOT NULL AND duoarea_bk IS NOT NULL")
@dp.expect_or_fail(
    "valid_consumption_value",
    "value IS NULL OR "
    "(try_cast(value AS decimal(10,3)) IS NOT NULL AND try_cast(value AS decimal(10,3)) >= 0)"
)
def petroleum_consumption_cleaned():
    bronze = spark.readStream.table("petroleum_consumption_raw_ldp_bronze")

    return (
        bronze
            .withColumnRenamed("series", "series_bk")
            .withColumnRenamed("duoarea", "duoarea_bk")
            .withColumnRenamed("product", "product_bk")
            .withColumn("period_bk", expr("try_cast(period as date)"))
            .withColumn("consumption_value", expr("try_cast(value as decimal(10,3))"))
            .select(
                "series_bk", "duoarea_bk", "period_bk", "product_bk",
                "units", "consumption_value", "_ingested_at"
            )
    )


dp.create_auto_cdc_flow(
    target="petroleum_consumption_ldp_silver",
    source="petroleum_consumption_cleaned",
    keys=["series_bk", "duoarea_bk", "period_bk"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1
)