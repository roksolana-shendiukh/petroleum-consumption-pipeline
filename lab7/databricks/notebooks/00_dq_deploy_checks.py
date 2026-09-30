# Databricks notebook source
import os
import sys

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("src_path", "")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = "/Workspace" + notebook_path.rsplit("/notebooks/", 1)[0]

config_path = dbutils.widgets.get("config_path") or f"{project_root}/config/pipeline_config.yaml"
src_path = dbutils.widgets.get("src_path") or f"{project_root}/src"
checks_dir = os.path.join(os.path.dirname(config_path), "dq_checks")

sys.dont_write_bytecode = True
sys.path.insert(0, src_path)

from petroleum_transformations.config import build_table_names, load_config
from petroleum_transformations.dq.rules import load_row_suite
from petroleum_transformations.dq.storage import deploy_checks

cfg = load_config(config_path, dbutils.widgets.get("environment"))
T = build_table_names(cfg)

# COMMAND ----------

dq = DQEngine(WorkspaceClient())
deploy_checks(dq, load_row_suite(dq, checks_dir, T, cfg["dq"]), T["dq_checks"])