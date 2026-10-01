# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import uuid
from datetime import datetime, timezone

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

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
from petroleum_transformations.dq.runner import create_dq_tables, run_suite, write_dq_outputs
from petroleum_transformations.dq.storage import load_suite

cfg = load_config(config_path, dbutils.widgets.get("environment"))
T = build_table_names(cfg)

RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)

# COMMAND ----------

dq = DQEngine(WorkspaceClient())
create_dq_tables(spark, T)
results_df, quarantine_df = run_suite(spark, dq, load_suite(spark, dq, T["dq_checks"], cfg), RUN_ID, RUN_TS)

# COMMAND ----------

write_dq_outputs(results_df, quarantine_df, T)