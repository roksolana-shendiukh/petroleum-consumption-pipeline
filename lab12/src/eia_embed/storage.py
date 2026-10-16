from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_chunks(spark: SparkSession, full_name: str) -> DataFrame:
    return spark.table(full_name)


def save_embeddings(frame: DataFrame, full_name: str) -> None:
    frame.write.mode("overwrite").insertInto(full_name)


def count_rows(spark: SparkSession, full_name: str) -> int:
    return spark.table(full_name).count()


def count_invalid(spark: SparkSession, full_name: str, dimension: int) -> int:
    return (
        spark.table(full_name)
        .filter(F.col("embedding").isNull() | (F.size("embedding") != dimension))
        .count()
    )