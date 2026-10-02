from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = spark.conf.get("petroleum.consumption_path")
SCHEMA_LOCATION = spark.conf.get("petroleum.consumption_schema_location")


@dp.table(
    name="petroleum_consumption_raw",
    comment="Weekly petroleum consumption files, ingested incrementally with Auto Loader",
    table_properties={"pipelines.reset.allowed": "false", "delta.appendOnly": "true"},
)
def petroleum_consumption_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(SOURCE_PATH)
        .withColumn("_source_filename", col("_metadata.file_path"))
        .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
    )