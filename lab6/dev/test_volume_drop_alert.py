# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import logging

logger = logging.getLogger("test_volume_drop_alert")
logger.setLevel(logging.INFO)

CATALOG = "dbr_dev_ua5816bd"
GOLD_SCHEMA = "roksolana_shendiu770_gold"
FACT_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fct_snapshot_consumption_weekly"
BACKUP_TABLE = f"{CATALOG}.{GOLD_SCHEMA}._backup_latest_week_consumption"

latest_week_row = spark.sql(f"""
    SELECT MAX(d.date_key) AS latest_date_key
    FROM {FACT_TABLE} c
    JOIN {CATALOG}.{GOLD_SCHEMA}.dim_date d ON c.dim_date_key = d.date_key
""").collect()[0]

latest_date_key = latest_week_row["latest_date_key"]
logger.info(f"Latest dim_date_key in fact table: {latest_date_key}")

backup_df = spark.sql(f"""
    SELECT * FROM {FACT_TABLE}
    WHERE dim_date_key = {latest_date_key}
""")

row_count_to_delete = backup_df.count()
logger.info(f"Rows to back up and delete: {row_count_to_delete}")

backup_df.write.format("delta").mode("overwrite").saveAsTable(BACKUP_TABLE)
logger.info(f"Backup saved to {BACKUP_TABLE}")

spark.sql(f"""
    DELETE FROM {FACT_TABLE}
    WHERE dim_date_key = {latest_date_key}
""")
logger.info(f"Deleted {row_count_to_delete} rows from {FACT_TABLE}")

# COMMAND ----------

restore_df = spark.table(BACKUP_TABLE)
restore_df.write.format("delta").mode("append").saveAsTable(FACT_TABLE)

spark.sql(f"DROP TABLE IF EXISTS {BACKUP_TABLE}")

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {FACT_TABLE}").show()

# COMMAND ----------

spark.sql(f"""
    SELECT COUNT(*) FROM {FACT_TABLE}
    WHERE dim_date_key = (
        SELECT MAX(d.date_key)
        FROM {FACT_TABLE} c
        JOIN dbr_dev_ua5816bd.roksolana_shendiu770_gold.dim_date d ON c.dim_date_key = d.date_key
    )
""").show()