from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

CONSUMPTION_PATH = spark.conf.get("petroleum.consumption_path")


@dp.materialized_view(
    name="petroleum_consumption_raw_ldp_bronze",
    table_properties={
        "pipelines.reset.allowed": "true"
    }
)
def petroleum_consumption_raw_ldp_bronze():
    return (
        spark.read
            .format("json")
            .option("multiLine", "true")
            .load(CONSUMPTION_PATH)
            .withColumn("_source_filename", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )