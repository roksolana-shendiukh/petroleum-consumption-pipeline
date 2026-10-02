# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %pip install databricks-labs-dqx==0.16.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
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

cfg = load_config(config_path, dbutils.widgets.get("environment"))
T = build_table_names(cfg)
Q = cfg["dq"]

ws = WorkspaceClient()
suite = load_row_suite(DQEngine(ws), checks_dir, T, Q)

# COMMAND ----------

OPTIONS = {"sample_fraction": None, "limit": None}

profiler = DQProfiler(ws)
generator = DQGenerator(ws)

profiled = {}
for table, ours in suite:
    stats, profiles = profiler.profile(spark.table(table), options=OPTIONS)
    profiled[table] = (stats, generator.generate_dq_rules(profiles), ours)

# COMMAND ----------

def signature(check):
    arguments = check["check"].get("arguments") or {}
    return check["check"]["function"], arguments.get("column")


rows = []
for table, (stats, candidates, ours) in profiled.items():
    name = table.split(".")[-1]
    ours_signatures = {signature(c) for c in ours if signature(c)[1]}
    candidate_signatures = {signature(c) for c in candidates if signature(c)[1]}
    for function, column in sorted(candidate_signatures - ours_signatures):
        rows.append((name, "suggested by profile, not in our rules", function, column))
    for function, column in sorted(ours_signatures - candidate_signatures):
        rows.append((name, "in our rules, not suggested by profile", function, column))

display(spark.createDataFrame(rows, "table_name string, difference string, function string, column string"))

# COMMAND ----------

threshold_rules = [
    ("prices_silver.price", T["prices_silver"], "price", Q["price_min"], Q["price_max"]),
    ("fct_prices_monthly.weeks_count", T["fct_prices_monthly"], "weeks_count",
     Q["weeks_per_month_min"], Q["weeks_per_month_max"]),
    ("fct_consumption_monthly.weeks_count", T["fct_consumption_monthly"], "weeks_count",
     Q["weeks_per_month_min"], Q["weeks_per_month_max"]),
]

rows = []
for label, table, column, low, high in threshold_rules:
    stats = profiled[table][0][column]
    observed_min, observed_max = float(stats["min"]), float(stats["max"])
    rows.append((label, float(low), float(high), observed_min, observed_max,
                 observed_min >= low and observed_max <= high))

display(spark.createDataFrame(
    rows,
    "rule string, config_min double, config_max double, observed_min double, observed_max double, "
    "within_config boolean",
))