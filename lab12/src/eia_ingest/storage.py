from __future__ import annotations

import logging

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def save_raw(spark: SparkSession, rows: list[tuple], full_name: str, schema_ddl: str) -> None:
    df = spark.createDataFrame(rows, schema=schema_ddl)
    df.write.mode("overwrite").insertInto(full_name)
    log.info("Saved %s rows to %s", len(rows), full_name)