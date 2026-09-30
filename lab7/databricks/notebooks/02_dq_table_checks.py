# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-labs-dqx",
# ]
# ///
# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("run_id", "")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = "/Workspace" + notebook_path.rsplit("/notebooks/", 1)[0]

config_path = dbutils.widgets.get("config_path") or f"{project_root}/config/pipeline_config.yaml"
src_path = dbutils.widgets.get("src_path") or f"{project_root}/src"

sys.dont_write_bytecode = True
sys.path.insert(0, src_path)

from petroleum_transformations.config import build_table_names, load_config
from petroleum_transformations.dq.checks import RunContext, run_table_checks
from petroleum_transformations.dq.runner import create_dq_tables, write_dq_outputs

cfg = load_config(config_path, dbutils.widgets.get("environment"))
T = build_table_names(cfg)

ctx = RunContext(
    dbutils.widgets.get("run_id") or str(uuid.uuid4()),
    datetime.now(timezone.utc),
)

# COMMAND ----------

create_dq_tables(spark, T)
results_df, quarantine_df = run_table_checks(spark, T, cfg["dq"], ctx)

# COMMAND ----------

write_dq_outputs(results_df, quarantine_df, T)