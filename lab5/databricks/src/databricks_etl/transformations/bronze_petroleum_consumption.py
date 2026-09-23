from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

CONSUMPTION_PATH = spark.conf.get("petroleum.consumption_path")
SCHEMA_LOCATION = spark.conf.get("petroleum.consumption_schema_location")


@dp.table(
    name="petroleum_consumption_raw_ldp_bronze",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)
def petroleum_consumption_raw_ldp_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("multiLine", "true")
            .load(CONSUMPTION_PATH)
            .withColumn("_source_filename", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )