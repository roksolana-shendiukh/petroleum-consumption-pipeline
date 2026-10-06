# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger("check_gold_tables")


# COMMAND ----------


dbutils.widgets.text("catalog", "")
dbutils.widgets.text("gold_schema", "")

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
if not catalog or not gold_schema:
    raise ValueError("Widgets 'catalog' and 'gold_schema' are required")

GOLD_TABLES = [
    "dim_product",
    "dim_date",
    "dim_area",
    "fct_snapshot_consumption_weekly",
    "fct_snapshot_consumption_monthly",
    "fct_snapshot_prices_weekly",
    "fct_snapshot_prices_monthly",
]

# COMMAND ----------

empty_tables = []
for table in GOLD_TABLES:
    full_name = f"{catalog}.{gold_schema}.{table}"
    row_count = spark.table(full_name).count()
    logger.info("%s: %d rows", full_name, row_count)
    if row_count == 0:
        logger.error("%s is empty", full_name)
        empty_tables.append(table)

if empty_tables:
    raise RuntimeError(f"Empty gold tables: {empty_tables}")

logger.info("All %d gold tables are not empty", len(GOLD_TABLES))