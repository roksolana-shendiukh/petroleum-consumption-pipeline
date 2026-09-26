# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging
import yaml

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")

environment = dbutils.widgets.get("environment")
CONFIG_PATH = dbutils.widgets.get("config_path")

logger = logging.getLogger("gold_dimensions_ddl")
logger.setLevel(logging.INFO)

try:
    with open(CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)

    config = full_config[environment]

    catalog = config["catalog"]
    gold_schema = config["gold_schema"]

    logger.info(f"[{environment}] Config loaded. Catalog: {catalog}, Gold schema: {gold_schema}")

except Exception as e:
    logger.error(f"Failed to load config from {CONFIG_PATH} for environment '{environment}': {e}")
    raise

# COMMAND ----------

import logging

logger = logging.getLogger("gold_facts_ddl")
logger.setLevel(logging.INFO)

try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fct_snapshot_consumption_weekly (
            dim_date_key BIGINT NOT NULL,
            dim_product_key BIGINT NOT NULL,
            dim_area_key BIGINT NOT NULL,
            series_bk STRING,
            consumption_value DECIMAL(10,3),
            CONSTRAINT pk_fct_consumption_weekly PRIMARY KEY (dim_date_key, dim_product_key, dim_area_key),
            CONSTRAINT fk_consumption_weekly_date FOREIGN KEY (dim_date_key) REFERENCES {catalog}.{gold_schema}.dim_date (date_key),
            CONSTRAINT fk_consumption_weekly_product FOREIGN KEY (dim_product_key) REFERENCES {catalog}.{gold_schema}.dim_product (product_key),
            CONSTRAINT fk_consumption_weekly_area FOREIGN KEY (dim_area_key) REFERENCES {catalog}.{gold_schema}.dim_area (area_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.fct_snapshot_consumption_weekly created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fct_snapshot_prices_weekly (
            dim_date_key BIGINT NOT NULL,
            dim_product_key BIGINT NOT NULL,
            series_bk STRING,
            price DECIMAL(10,3),
            CONSTRAINT pk_fct_prices_weekly PRIMARY KEY (dim_date_key, dim_product_key),
            CONSTRAINT fk_prices_weekly_date FOREIGN KEY (dim_date_key) REFERENCES {catalog}.{gold_schema}.dim_date (date_key),
            CONSTRAINT fk_prices_weekly_product FOREIGN KEY (dim_product_key) REFERENCES {catalog}.{gold_schema}.dim_product (product_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.fct_snapshot_prices_weekly created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fct_snapshot_consumption_monthly (
            dim_date_key BIGINT NOT NULL,
            dim_product_key BIGINT NOT NULL,
            dim_area_key BIGINT NOT NULL,
            total_consumption DECIMAL(12,3),
            avg_weekly_consumption DECIMAL(10,3),
            min_weekly_consumption DECIMAL(10,3),
            max_weekly_consumption DECIMAL(10,3),
            weeks_count INT,
            CONSTRAINT pk_fct_consumption_monthly PRIMARY KEY (dim_date_key, dim_product_key, dim_area_key),
            CONSTRAINT fk_consumption_monthly_date FOREIGN KEY (dim_date_key) REFERENCES {catalog}.{gold_schema}.dim_date (date_key),
            CONSTRAINT fk_consumption_monthly_product FOREIGN KEY (dim_product_key) REFERENCES {catalog}.{gold_schema}.dim_product (product_key),
            CONSTRAINT fk_consumption_monthly_area FOREIGN KEY (dim_area_key) REFERENCES {catalog}.{gold_schema}.dim_area (area_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.fct_snapshot_consumption_monthly created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fct_snapshot_prices_monthly (
            dim_date_key BIGINT NOT NULL,
            dim_product_key BIGINT NOT NULL,
            avg_price DECIMAL(10,3),
            min_price DECIMAL(10,3),
            max_price DECIMAL(10,3),
            price_volatility DECIMAL(10,3),
            weeks_count INT,
            CONSTRAINT pk_fct_prices_monthly PRIMARY KEY (dim_date_key, dim_product_key),
            CONSTRAINT fk_prices_monthly_date FOREIGN KEY (dim_date_key) REFERENCES {catalog}.{gold_schema}.dim_date (date_key),
            CONSTRAINT fk_prices_monthly_product FOREIGN KEY (dim_product_key) REFERENCES {catalog}.{gold_schema}.dim_product (product_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.fct_snapshot_prices_monthly created or already exists.")

except Exception as e:
    logger.error(f"DDL execution failed: {e}")
    raise