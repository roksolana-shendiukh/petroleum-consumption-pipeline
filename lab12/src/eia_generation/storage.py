from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_questions(spark: SparkSession, full_name: str, splits: list[str]) -> list[dict[str, Any]]:
    rows = spark.table(full_name).filter(F.col("split").isin(splits)).orderBy("question_id").collect()
    return [row.asDict() for row in rows]