from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("petroleum.catalog")
GOLD = spark.conf.get("petroleum.gold_schema")
CALENDAR_START = spark.conf.get("petroleum.calendar_start")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_date",
    comment="Daily calendar from the start of the history to today",
)
def dim_date():
    days = spark.range(1).select(
        F.explode(
            F.sequence(F.lit(CALENDAR_START).cast("date"), F.current_date(), F.expr("interval 1 day"))
        ).alias("full_date")
    )
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("bigint").alias("date_key"),
        "full_date",
        F.date_trunc("week", "full_date").cast("date").alias("week_start_date"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").alias("quarter"),
        F.year("full_date").alias("year"),
    )