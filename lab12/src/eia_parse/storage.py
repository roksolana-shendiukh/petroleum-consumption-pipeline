from typing import Any

from pyspark.sql import Row, SparkSession

from lab12_tables.definitions import column_names, columns_ddl


def read_raw(spark: SparkSession, full_name: str) -> list[Row]:
    return spark.table(full_name).orderBy("doc_id").collect()


def save_parsed(spark: SparkSession, rows: list[dict[str, Any]], full_name: str, table: dict[str, Any]) -> None:
    names = column_names(table)
    data = [tuple(row[name] for name in names) for row in rows]
    frame = spark.createDataFrame(data, schema=columns_ddl(table))
    frame.write.mode("overwrite").insertInto(full_name)