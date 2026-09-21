from pyspark import pipelines as dp
from pyspark.sql.functions import expr, struct

dp.create_streaming_table("petroleum_prices_ldp_silver")


@dp.view(name="petroleum_prices_cleaned")
@dp.expect_or_drop("valid_effective_from", "effective_from IS NOT NULL")
@dp.expect_or_drop("valid_series", "series_bk IS NOT NULL")
@dp.expect_or_fail("valid_price", "price IS NULL OR price > 0")
def petroleum_prices_cleaned():
    bronze = spark.readStream.table("petroleum_prices_raw_ldp_bronze")

    return (
        bronze
            .withColumnRenamed("series", "series_bk")
            .withColumnRenamed("product-name", "product_name")
            .withColumn("effective_from", expr("try_cast(period as date)"))
            .withColumn("price", expr("try_cast(value as decimal(10,3))"))
            .select(
                "series_bk", "product_name", "units", "price",
                "effective_from", "_ingested_at"
            )
    )


dp.create_auto_cdc_flow(
    target="petroleum_prices_ldp_silver",
    source="petroleum_prices_cleaned",
    keys=["series_bk"],
    sequence_by=struct("effective_from", "_ingested_at"),
    stored_as_scd_type=2
)