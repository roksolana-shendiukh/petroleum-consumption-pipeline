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
from pyspark.sql.functions import col, date_trunc, month, quarter, year, date_format
from delta.tables import DeltaTable

logger = logging.getLogger("gold_dimensions_pipeline")
logger.setLevel(logging.INFO)

try:
    consumption_dates = spark.table(consumption_silver).select(col("period_bk").alias("full_date"))
    prices_dates = spark.table(prices_silver).select(col("effective_from").alias("full_date"))

    all_dates = consumption_dates.union(prices_dates).distinct().filter(col("full_date").isNotNull())

    dim_date_df = (all_dates
        .withColumn("week_start_date", date_trunc("week", col("full_date")).cast("date"))
        .withColumn("month", month(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("year", year(col("full_date")))
    )

    target_table_obj = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_date")

    merge_result = (target_table_obj.alias("t")
        .merge(dim_date_df.alias("s"), "t.full_date = s.full_date")
        .whenNotMatchedInsert(values={
            "full_date": "s.full_date",
            "week_start_date": "s.week_start_date",
            "month": "s.month",
            "month_name": "s.month_name",
            "quarter": "s.quarter",
            "year": "s.year"
        })
        .execute()
    )

    stats = merge_result.collect()[0].asDict()
    logger.info(f"dim_date MERGE completed: {stats}")

except Exception as e:
    logger.error(f"dim_date load failed: {e}")
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
        .select(col("duoarea_bk").alias("area_code"))
        .distinct()
        .filter(col("area_code").isNotNull())
        .withColumn("area_name", lit("Unknown"))
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