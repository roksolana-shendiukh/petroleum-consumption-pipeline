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
from pyspark.sql.functions import col, broadcast
from delta.tables import DeltaTable

logger = logging.getLogger("gold_facts_weekly_pipeline")
logger.setLevel(logging.INFO)

try:
    consumption_df = spark.table(consumption_silver)
    dim_date_df = spark.table(dim_date_tbl).select("date_key", "full_date")
    dim_product_df = spark.table(dim_product_tbl).select("product_key", "product_code")
    dim_area_df = spark.table(dim_area_tbl).select("area_key", "area_code")

    joined_df = (consumption_df
        .join(broadcast(dim_date_df), consumption_df.period_bk == dim_date_df.full_date, "left")
        .join(broadcast(dim_product_df), consumption_df.product_bk == dim_product_df.product_code, "left")
        .join(broadcast(dim_area_df), consumption_df.duoarea_bk == dim_area_df.area_code, "left")
    )

    orphans_df = (joined_df
        .filter(col("date_key").isNull() | col("product_key").isNull() | col("area_key").isNull())
        .select("series_bk", "period_bk", "product_bk", "duoarea_bk")
    )

    orphans_df.cache()
    if not orphans_df.isEmpty():
        logger.warning("Some consumption rows could not be resolved against dimensions. Sample below.")
        orphans_df.show(20, truncate=False)
    orphans_df.unpersist()

    fact_df = (joined_df
        .filter(
            col("date_key").isNotNull() & col("product_key").isNotNull() & col("area_key").isNotNull()
        )
        .select(
            col("date_key").alias("dim_date_key"),
            col("product_key").alias("dim_product_key"),
            col("area_key").alias("dim_area_key"),
            col("series_bk"),
            col("consumption_value")
        )
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fct_snapshot_consumption_weekly")

    merge_result = (target_table_obj.alias("t")
        .merge(
            fact_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key AND t.dim_area_key = s.dim_area_key"
        )
        .whenMatchedUpdate(set={
            "series_bk": "s.series_bk",
            "consumption_value": "s.consumption_value"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"fct_snapshot_consumption_weekly MERGE completed: {stats}")

except Exception as e:
    logger.error(f"fct_snapshot_consumption_weekly load failed: {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col, split, broadcast
from delta.tables import DeltaTable

logger = logging.getLogger("gold_facts_weekly_pipeline")
logger.setLevel(logging.INFO)

try:
    prices_df = spark.table(prices_silver).withColumn(
        "parsed_product_code", split(col("series_bk"), "_").getItem(1)
    )
    dim_date_df = spark.table(dim_date_tbl).select("date_key", "full_date")
    dim_product_df = spark.table(dim_product_tbl).select("product_key", "product_code")

    joined_df = (prices_df
        .join(broadcast(dim_date_df), prices_df.effective_from == dim_date_df.full_date, "left")
        .join(broadcast(dim_product_df), prices_df.parsed_product_code == dim_product_df.product_code, "left")
    )

    orphans_df = (joined_df
        .filter(col("date_key").isNull() | col("product_key").isNull())
        .select("series_bk", "effective_from", "parsed_product_code")
    )

    orphans_df.cache()
    if not orphans_df.isEmpty():
        logger.warning("Some prices rows could not be resolved against dimensions. Sample below.")
        orphans_df.show(20, truncate=False)
    orphans_df.unpersist()

    fact_df = (joined_df
        .filter(col("date_key").isNotNull() & col("product_key").isNotNull())
        .select(
            col("date_key").alias("dim_date_key"),
            col("product_key").alias("dim_product_key"),
            col("series_bk"),
            col("price")
        )
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fct_snapshot_prices_weekly")

    merge_result = (target_table_obj.alias("t")
        .merge(
            fact_df.alias("s"),
            "t.dim_date_key = s.dim_date_key AND t.dim_product_key = s.dim_product_key"
        )
        .whenMatchedUpdate(set={
            "series_bk": "s.series_bk",
            "price": "s.price"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"fct_snapshot_prices_weekly MERGE completed: {stats}")

except Exception as e:
    logger.error(f"fct_snapshot_prices_weekly load failed: {e}")
    raise