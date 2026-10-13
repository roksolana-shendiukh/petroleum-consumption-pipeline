from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_generations(spark: SparkSession, full_name: str, run: str) -> tuple[str, list[dict[str, Any]]]:
    table = spark.table(full_name)
    if run == "latest":
        latest = table.orderBy(F.col("generated_at").desc()).select("run_id").first()
        if latest is None:
            raise RuntimeError("No generation results found")
        run = latest["run_id"]
    rows = table.filter((F.col("run_id") == run) & F.col("answer").isNotNull()).orderBy("question_id", "mode").collect()
    return run, [row.asDict() for row in rows]


def read_question_map(spark: SparkSession, full_name: str) -> dict[str, dict[str, Any]]:
    return {row["question_id"]: row.asDict() for row in spark.table(full_name).collect()}


def read_chunks(spark: SparkSession, full_name: str, chunk_ids: list[str]) -> dict[str, dict[str, Any]]:
    rows = spark.table(full_name).filter(F.col("chunk_id").isin(chunk_ids)).collect()
    return {row["chunk_id"]: row.asDict() for row in rows}