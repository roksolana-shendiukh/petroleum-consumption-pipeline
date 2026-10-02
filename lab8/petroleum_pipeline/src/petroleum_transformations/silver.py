from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, expr, lead, lit, row_number, sha2
from pyspark.sql.window import Window


def clean_consumption(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .withColumnRenamed("series", "series_bk")
        .withColumnRenamed("duoarea", "duoarea_bk")
        .withColumnRenamed("area-name", "area_name")
        .withColumnRenamed("product", "product_bk")
        .withColumn("period_bk", expr("try_cast(period as date)"))
        .withColumn("consumption_value", expr("try_cast(value as decimal(10,3))"))
        .select(
            "series_bk", "duoarea_bk", "area_name", "period_bk", "product_bk",
            "units", "consumption_value", "source_filename", "ingestion_timestamp",
        )
    )


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def add_surrogate_key(df: DataFrame, key_cols: list[str], sk_name: str) -> DataFrame:
    parts = [col(c).cast("string") for c in key_cols]
    return df.withColumn(sk_name, sha2(concat_ws("||", *parts), 256))


def build_silver_consumption(bronze_df: DataFrame) -> DataFrame:
    cleaned = clean_consumption(bronze_df)
    deduped = dedupe_latest(
        cleaned, ["series_bk", "duoarea_bk", "period_bk"], "ingestion_timestamp"
    )
    with_sk = add_surrogate_key(
        deduped, ["series_bk", "duoarea_bk", "period_bk"], "consumption_sk"
    )
    return (
        with_sk
        .withColumn("_source_system", lit("EIA_petroleum_consumption"))
        .withColumn("_ingested_at", expr("current_timestamp()"))
        .withColumn("_updated_at", lit(None).cast("timestamp"))
        .select(
            "consumption_sk", "series_bk", "duoarea_bk", "area_name", "period_bk", "product_bk",
            "units", "consumption_value", "_source_system", "_ingested_at", "_updated_at",
        )
    )

def clean_prices(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .withColumnRenamed("series", "series_bk")
        .withColumnRenamed("product", "product_bk")
        .withColumnRenamed("product-name", "product_name")
        .withColumn("effective_from", expr("try_cast(period as date)"))
        .withColumn("price", expr("try_cast(value as decimal(10,3))"))
        .select(
            "series_bk", "product_bk", "product_name", "units", "price", "effective_from",
            "source_filename", "ingestion_timestamp",
        )
        .filter(col("price").isNotNull() & (col("price") > 0))
    )


def add_validity_window(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("series_bk").orderBy("effective_from")
    return (
        df.withColumn("effective_to", lead("effective_from", 1).over(w))
        .withColumn("is_current", col("effective_to").isNull())
    )


def earliest_per_series(batch_df: DataFrame) -> DataFrame:
    w = Window.partitionBy("series_bk").orderBy("effective_from")
    return (
        batch_df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select("series_bk", "effective_from", "price")
    )


def build_silver_prices(bronze_df: DataFrame) -> DataFrame:
    cleaned = clean_prices(bronze_df)
    deduped = dedupe_latest(cleaned, ["series_bk", "effective_from"], "ingestion_timestamp")
    with_sk = add_surrogate_key(deduped, ["series_bk", "effective_from"], "price_sk")
    return (
        add_validity_window(with_sk)
        .withColumn("_source_system", lit("EIA_petroleum_prices"))
        .withColumn("_ingested_at", expr("current_timestamp()"))
        .withColumn("_updated_at", lit(None).cast("timestamp"))
        .select(
            "price_sk", "series_bk", "product_bk", "product_name", "units", "price",
            "effective_from", "effective_to", "is_current",
            "_source_system", "_ingested_at", "_updated_at",
        )
    )
