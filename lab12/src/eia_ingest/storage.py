from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

log = logging.getLogger(__name__)

SCHEMA = StructType(
    [
        StructField("doc_id", StringType(), False),
        StructField("source", StringType(), False),
        StructField("doc_type", StringType(), False),
        StructField("section", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("source_url", StringType(), False),
        StructField("final_url", StringType(), False),
        StructField("raw_html", StringType(), False),
        StructField("content_hash", StringType(), False),
        StructField("retrieved_at", TimestampType(), False),
    ]
)


def save_raw(spark: SparkSession, rows: list[tuple], full_name: str) -> None:
    df = spark.createDataFrame(rows, schema=SCHEMA)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    log.info("Saved %s rows to %s", len(rows), full_name)