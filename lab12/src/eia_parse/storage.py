from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, TimestampType

SCHEMA = StructType([
    StructField("doc_id", StringType(), False),
    StructField("part_index", IntegerType(), False),
    StructField("part_heading", StringType(), False),
    StructField("part_text", StringType(), False),
    StructField("char_count", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("source", StringType(), False),
    StructField("doc_type", StringType(), False),
    StructField("section", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("source_url", StringType(), False),
    StructField("last_updated", StringType(), True),
    StructField("content_hash", StringType(), False),
    StructField("retrieved_at", TimestampType(), False),
    StructField("parsed_at", TimestampType(), False),
])


def read_raw(spark: SparkSession, full_name: str) -> list[Row]:
    return spark.table(full_name).orderBy("doc_id").collect()


def save_parsed(spark: SparkSession, rows: list[dict[str, Any]], full_name: str) -> None:
    data = [tuple(row[name] for name in SCHEMA.names) for row in rows]
    frame = spark.createDataFrame(data, SCHEMA)
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)