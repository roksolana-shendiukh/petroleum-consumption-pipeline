# Databricks notebook source
import logging
import yaml

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")

environment = dbutils.widgets.get("environment")
CONFIG_PATH = dbutils.widgets.get("config_path")

logger = logging.getLogger("gold_facts_weekly_pipeline")
logger.setLevel(logging.INFO)

try:
    with open(CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)

    config = full_config[environment]

    catalog = config["catalog"]
    silver_schema = config["silver_schema"]
    gold_schema = config["gold_schema"]

    consumption_silver = f"{catalog}.{silver_schema}.{config['consumption']['target_table']}"
    prices_silver = f"{catalog}.{silver_schema}.{config['prices']['target_table']}"

    dim_date_tbl = f"{catalog}.{gold_schema}.dim_date"
    dim_product_tbl = f"{catalog}.{gold_schema}.dim_product"
    dim_area_tbl = f"{catalog}.{gold_schema}.dim_area"

    logger.info(f"[{environment}] Config loaded. Sources: {consumption_silver}, {prices_silver}")

except Exception as e:
    logger.error(f"Failed to load config from {CONFIG_PATH} for environment '{environment}': {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col, min as spark_min, sum as spark_sum, avg, max as spark_max, count
from delta.tables import DeltaTable

logger = logging.getLogger("gold_facts_monthly_pipeline")
logger.setLevel(logging.INFO)

try:
    weekly_df = spark.table(f"{catalog}.{gold_schema}.fct_snapshot_consumption_weekly")
    dim_date_df = spark.table(f"{catalog}.{gold_schema}.dim_date").select(
        col("date_key").alias("dim_date_key"), "month", "year"
    )

    monthly_df = (weekly_df
        .join(dim_date_df, "dim_date_key")
        .groupBy("dim_product_key", "dim_area_key", "year", "month")
        .agg(
            spark_min("dim_date_key").alias("dim_date_key"),
            spark_sum("consumption_value").alias("total_consumption"),
            avg("consumption_value").alias("avg_weekly_consumption"),
            spark_min("consumption_value").alias("min_weekly_consumption"),
            spark_max("consumption_value").alias("max_weekly_consumption"),
            count("consumption_value").alias("weeks_count")
        )
        .select(
            "dim_date_key", "dim_product_key", "dim_area_key",
            "total_consumption", "avg_weekly_consumption",
            "min_weekly_consumption", "max_weekly_consumption", "weeks_count"
        )
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fct_snapshot_consumption_monthly")

    merge_result = (target_table_obj.alias("t")
        .merge(
            monthly_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key AND t.dim_area_key = s.dim_area_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"fct_snapshot_consumption_monthly MERGE completed: {stats}")

except Exception as e:
    logger.error(f"fct_snapshot_consumption_monthly load failed: {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col, min as spark_min, avg, max as spark_max, count
from delta.tables import DeltaTable

logger = logging.getLogger("gold_facts_monthly_pipeline")
logger.setLevel(logging.INFO)

try:
    weekly_df = spark.table(f"{catalog}.{gold_schema}.fct_snapshot_prices_weekly")
    dim_date_df = spark.table(f"{catalog}.{gold_schema}.dim_date").select(
        col("date_key").alias("dim_date_key"), "month", "year"
    )

    monthly_df = (weekly_df
        .join(dim_date_df, "dim_date_key")
        .groupBy("dim_product_key", "year", "month")
        .agg(
            spark_min("dim_date_key").alias("dim_date_key"),
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            count("price").alias("weeks_count")
        )
        .withColumn("price_volatility", col("max_price") - col("min_price"))
        .select(
            "dim_date_key", "dim_product_key",
            "avg_price", "min_price", "max_price", "price_volatility", "weeks_count"
        )
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fct_snapshot_prices_monthly")

    merge_result = (target_table_obj.alias("t")
        .merge(
            monthly_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"fct_snapshot_prices_monthly MERGE completed: {stats}")

except Exception as e:
    logger.error(f"fct_snapshot_prices_monthly load failed: {e}")
    raise