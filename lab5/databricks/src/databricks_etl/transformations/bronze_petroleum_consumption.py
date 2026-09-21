from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

RAW_FILES_PATH = spark.conf.get("petroleum.raw_files_path")
CONSUMPTION_FILE = spark.conf.get("petroleum.consumption_file")


@dp.table(
    name="petroleum_consumption_raw_ldp_bronze",
    comment="Raw petroleum consumption data ingested from EIA API JSON files (Lakeflow Declarative Pipelines version)"
)
def petroleum_consumption_raw_ldp_bronze():
    return (
        spark.read
            .format("json")
            .option("multiLine", "true")
            .load(f"{RAW_FILES_PATH}/{CONSUMPTION_FILE}")
            .withColumn("_source_filename", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )