# Databricks notebook source
import logging
import yaml

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")

environment = dbutils.widgets.get("environment")
CONFIG_PATH = dbutils.widgets.get("config_path")

logger = logging.getLogger("gold_dimensions_pipeline")
logger.setLevel(logging.INFO)

try:
    with open(CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)

    config = full_config[environment]

    catalog = config["catalog"]
    bronze_schema = config["bronze_schema"]
    silver_schema = config["silver_schema"]
    gold_schema = config["gold_schema"]

    consumption_silver = f"{catalog}.{silver_schema}.{config['consumption']['target_table']}"
    prices_silver = f"{catalog}.{silver_schema}.{config['prices']['target_table']}"

    logger.info(f"[{environment}] Config loaded. Sources: {consumption_silver}, {prices_silver}")

except Exception as e:
    logger.error(f"Failed to load config from {CONFIG_PATH} for environment '{environment}': {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col
from delta.tables import DeltaTable

logger = logging.getLogger("gold_dimensions_pipeline")
logger.setLevel(logging.INFO)

try:
    mapping_df = (
        spark.table(f"{catalog}.{bronze_schema}.product_price_mapping")
        .select(
            col("consumption_product_code"),
            col("consumption_product_name").alias("product_name"),
            col("price_product_code")
        )
    )

    consumption_units = (
        spark.table(consumption_silver)
        .select(
            col("product_bk").alias("consumption_product_code"),
            col("units").alias("unit_code")
        )
        .distinct()
    )

    dim_product_df = (
        mapping_df
        .join(consumption_units, on="consumption_product_code", how="left")
        .select(
            "consumption_product_code",
            "price_product_code",
            "product_name",
            "unit_code"
        )
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_product")

    merge_result = (target_table_obj.alias("t")
        .merge(dim_product_df.alias("s"), "t.consumption_product_code = s.consumption_product_code")
        .whenMatchedUpdate(set={
            "price_product_code": "s.price_product_code",
            "product_name": "s.product_name",
            "unit_code": "s.unit_code"
        })
        .whenNotMatchedInsert(values={
            "consumption_product_code": "s.consumption_product_code",
            "price_product_code": "s.price_product_code",
            "product_name": "s.product_name",
            "unit_code": "s.unit_code"
        })
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"dim_product MERGE completed: {stats}")

except Exception as e:
    logger.error(f"dim_product load failed: {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col, split, lit, when, countDistinct, first
from delta.tables import DeltaTable

logger = logging.getLogger("gold_dimensions_pipeline")
logger.setLevel(logging.INFO)

try:
    consumption_products = (spark.table(consumption_silver)
        .select(
            col("product_bk").alias("product_code"),
            lit("Unknown").cast("string").alias("product_name"),
            col("units").alias("unit_code")
        )
        .distinct()
        .withColumn("source_system", lit("consumption"))
    )

    prices_products = (spark.table(prices_silver)
        .withColumn("product_code", split(col("series_bk"), "_").getItem(1))
        .select(
            "product_code",
            col("product_name"),
            col("units").alias("unit_code")
        )
        .distinct()
        .withColumn("source_system", lit("prices"))
    )

    combined_products = consumption_products.unionByName(prices_products)


    dim_product_df = (combined_products
        .groupBy("product_code")
        .agg(
            first(when(col("source_system") == "prices", col("product_name")), ignorenulls=True).alias("prices_name"),
            first("product_name", ignorenulls=True).alias("fallback_name"),
            first("unit_code", ignorenulls=True).alias("unit_code"),
            countDistinct("source_system").alias("source_count"),
            first("source_system", ignorenulls=True).alias("single_source")
        )
        .withColumn(
            "product_name",
            when(col("prices_name").isNotNull(), col("prices_name"))
            .otherwise(col("product_code")) 
        )
        .withColumn(
            "source_system",
            when(col("source_count") > 1, lit("both")).otherwise(col("single_source"))
        )
        .select("product_code", "product_name", "unit_code", "source_system")
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_product")

    merge_result = (target_table_obj.alias("t")
        .merge(dim_product_df.alias("s"), "t.product_code = s.product_code")
        .whenMatchedUpdate(set={
            "product_name": "s.product_name",
            "unit_code": "s.unit_code",
            "source_system": "s.source_system"
        })
        .whenNotMatchedInsert(values={
            "product_code": "s.product_code",
            "product_name": "s.product_name",
            "unit_code": "s.unit_code",
            "source_system": "s.source_system"
        })
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"dim_product MERGE completed: {stats}")

except Exception as e:
    logger.error(f"dim_product load failed: {e}")
    raise

# COMMAND ----------

import logging
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

logger = logging.getLogger("gold_dimensions_pipeline")
logger.setLevel(logging.INFO)

try:
    dim_area_df = (spark.table(consumption_silver)
        .select(
            col("duoarea_bk").alias("area_code"),
            col("area_name")
        )
        .distinct()
        .filter(col("area_code").isNotNull())
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_area")

    merge_result = (target_table_obj.alias("t")
        .merge(dim_area_df.alias("s"), "t.area_code = s.area_code")
        .whenNotMatchedInsert(values={
            "area_code": "s.area_code",
            "area_name": "s.area_name"
        })
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"dim_area MERGE completed: {stats}")

except Exception as e:
    logger.error(f"dim_area load failed: {e}")
    raise