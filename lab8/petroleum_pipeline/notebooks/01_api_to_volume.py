# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %pip install httpx tenacity

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
import os
import sys
from datetime import date, datetime, timedelta

import httpx

dbutils.widgets.text("volume_path", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("secret_scope", "eia_api")
dbutils.widgets.text("secret_key", "eia-api-key")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = "/Workspace" + notebook_path.rsplit("/notebooks/", 1)[0]

sys.dont_write_bytecode = True
sys.path.insert(0, f"{project_root}/src")

from petroleum_transformations.eia_client import run_sync
from petroleum_transformations.ingestion import collect, file_name, to_jsonl

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("eia_ingestion")

volume = dbutils.widgets.get("volume_path").rstrip("/")
if not volume:
    raise ValueError("The widget 'volume_path' is required")

today = date.today()
start = dbutils.widgets.get("start_date") or (today - timedelta(days=30)).isoformat()
end = dbutils.widgets.get("end_date") or today.isoformat()
stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
api_key = dbutils.secrets.get(
    scope=dbutils.widgets.get("secret_scope"), key=dbutils.widgets.get("secret_key")
)

# COMMAND ----------

async def download():
    async with httpx.AsyncClient() as client:
        return await collect(client, start, end, api_key)


consumption, prices, unmapped = run_sync(download())

if unmapped:
    logger.warning(f"No active price mapping for: {unmapped}")
logger.info(f"Downloaded {len(consumption)} consumption and {len(prices)} price rows for {start}..{end}")

# COMMAND ----------

def save(records, subfolder, prefix):
    if not records:
        logger.warning(f"No records for {prefix}, nothing saved")
        return None
    path = f"{volume}/{subfolder}/{file_name(prefix, start, end, stamp)}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(to_jsonl(records))
    logger.info(f"Saved {len(records)} records to {path}")
    return path
