# Databricks notebook source
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

logger = logging.getLogger("gold_dimensions_ddl")
logger.setLevel(logging.INFO)

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    logger.info(f"Schema {catalog}.{gold_schema} ensured.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_date (
            date_key INT GENERATED ALWAYS AS IDENTITY,
            full_date DATE NOT NULL,
            week_start_date DATE NOT NULL,
            month INT NOT NULL,
            month_name STRING NOT NULL,
            quarter INT NOT NULL,
            year INT NOT NULL,
            CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
        )
        USING DELTA
        COMMENT 'Gold dimension: calendar attributes at week grain'
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_date created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_product (
            product_key BIGINT GENERATED ALWAYS AS IDENTITY,
            product_code STRING NOT NULL,
            product_name STRING,
            unit_code STRING,
            source_system STRING NOT NULL,
            CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
        )
        USING DELTA
        COMMENT 'Gold dimension: EIA petroleum product reference'
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_product created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_area (
            area_key BIGINT GENERATED ALWAYS AS IDENTITY,
            area_code STRING NOT NULL,
            area_name STRING,
            CONSTRAINT pk_dim_area PRIMARY KEY (area_key)
        )
        USING DELTA
        COMMENT 'Gold dimension: EIA duoarea reference (consumption only)'
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_area created or already exists.")

except Exception as e:
    logger.error(f"DDL execution failed: {e}")
    raise

# COMMAND ----------

import logging

logger = logging.getLogger("gold_dimensions_ddl")
logger.setLevel(logging.INFO)

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    logger.info(f"Schema {catalog}.{gold_schema} ensured.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_date (
            date_key BIGINT GENERATED ALWAYS AS IDENTITY,
            full_date DATE NOT NULL,
            week_start_date DATE NOT NULL,
            month INT NOT NULL,
            month_name STRING NOT NULL,
            quarter INT NOT NULL,
            year INT NOT NULL,
            CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_date created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_product (
            product_key BIGINT GENERATED ALWAYS AS IDENTITY,
            product_code STRING NOT NULL,
            product_name STRING,
            unit_code STRING,
            source_system STRING NOT NULL,
            CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_product created or already exists.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_area (
            area_key BIGINT GENERATED ALWAYS AS IDENTITY,
            area_code STRING NOT NULL,
            area_name STRING,
            CONSTRAINT pk_dim_area PRIMARY KEY (area_key)
        )
    """)
    logger.info(f"Table {catalog}.{gold_schema}.dim_area created or already exists.")

except Exception as e:
    logger.error(f"DDL execution failed: {e}")
    raise