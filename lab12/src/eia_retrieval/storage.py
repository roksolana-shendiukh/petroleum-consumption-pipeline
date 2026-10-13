from typing import Any

from pyspark.sql import SparkSession

from lab12_tables.definitions import column_names, columns_ddl


def read_answerable_questions(spark: SparkSession, full_name: str) -> list[dict[str, Any]]:
    rows = spark.table(full_name).filter("answerable").orderBy("question_id").collect()
    return [row.asDict() for row in rows]


def save_results(spark: SparkSession, rows: list[dict[str, Any]], full_name: str, table: dict[str, Any]) -> None:
    names = column_names(table)
    data = [tuple(row[name] for name in names) for row in rows]
    frame = spark.createDataFrame(data, schema=columns_ddl(table))
    frame.write.mode("append").insertInto(full_name)