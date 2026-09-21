from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

RAW_FILES_PATH = spark.conf.get("petroleum.raw_files_path")
PRICES_FILE = spark.conf.get("petroleum.prices_file")


@dp.table(
    name="petroleum_prices_raw_ldp_bronze",
    comment="Raw petroleum prices data ingested from EIA API JSON files (Lakeflow Declarative Pipelines version)"
)
def petroleum_prices_raw_ldp_bronze():
    return (
        spark.read
            .format("json")
            .option("multiLine", "true")
            .load(f"{RAW_FILES_PATH}/{PRICES_FILE}")
            .withColumn("_source_filename", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )