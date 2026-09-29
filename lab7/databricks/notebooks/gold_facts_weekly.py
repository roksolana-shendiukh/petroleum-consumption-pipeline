# Databricks notebook source
import logging
import sys

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("src_path", "") 

environment = dbutils.widgets.get("environment")
CONFIG_PATH = dbutils.widgets.get("config_path")
SRC_PATH = dbutils.widgets.get("src_path")

sys.dont_write_bytecode = True
if SRC_PATH:
    sys.path.insert(0, SRC_PATH)

from petroleum_transformations.config import build_table_names, load_config

logger = logging.getLogger("gold_facts_weekly_pipeline")
logger.setLevel(logging.INFO)

try:
    config = load_config(CONFIG_PATH, environment)
    tables = build_table_names(config)
    logger.info(f"[{environment}] Config loaded.")
except Exception as e:
    logger.error(f"Failed to load config from {CONFIG_PATH} for environment '{environment}': {e}")
    raise

# COMMAND ----------

from delta.tables import DeltaTable
from petroleum_transformations.gold import build_fact_consumption_weekly

try:
    fact_df = build_fact_consumption_weekly(
        spark.table(tables["consumption_silver"]),
        spark.table(tables["dim_date"]),
        spark.table(tables["dim_product"]),
        spark.table(tables["dim_area"]),
    )

    merge_result = (
        DeltaTable.forName(spark, tables["fct_consumption_weekly"]).alias("t")
        .merge(
            fact_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key "
            "AND t.dim_area_key = s.dim_area_key",
        )
        .whenMatchedUpdate(set={
            "series_bk": "s.series_bk",
            "consumption_value": "s.consumption_value",
        })
        .whenNotMatchedInsert(values={
            "dim_date_key": "s.dim_date_key",
            "dim_product_key": "s.dim_product_key",
            "dim_area_key": "s.dim_area_key",
            "series_bk": "s.series_bk",
            "consumption_value": "s.consumption_value",
        })
        .execute()
    )
    logger.info(f"fct_snapshot_consumption_weekly MERGE completed: {merge_result.collect()[0].asDict()}")

except Exception as e:
    logger.error(f"fct_snapshot_consumption_weekly load failed: {e}")
    raise

# COMMAND ----------

from petroleum_transformations.gold import build_fact_prices_weekly

try:
    fact_df = build_fact_prices_weekly(
        spark.table(tables["prices_silver"]),
        spark.table(tables["dim_date"]),
        spark.table(tables["dim_product"]),
    )

    # series_bk is part of the grain: EIA returns one price per region (series) per product per week
    merge_result = (
        DeltaTable.forName(spark, tables["fct_prices_weekly"]).alias("t")
        .merge(
            fact_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key "
            "AND t.series_bk = s.series_bk",
        )
        .whenMatchedUpdate(set={
            "price": "s.price",
        })
        .whenNotMatchedInsert(values={
            "dim_date_key": "s.dim_date_key",
            "dim_product_key": "s.dim_product_key",
            "series_bk": "s.series_bk",
            "price": "s.price",
        })
        .execute()
    )
    logger.info(f"fct_snapshot_prices_weekly MERGE completed: {merge_result.collect()[0].asDict()}")

except Exception as e:
    logger.error(f"fct_snapshot_prices_weekly load failed: {e}")
    raise