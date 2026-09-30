# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import uuid
from datetime import datetime, timezone

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

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

environment = dbutils.widgets.get("environment")
cfg = load_config(config_path, environment)
T = build_table_names(cfg)
Q = cfg["dq"]

RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)
dq = DQEngine(WorkspaceClient())

# COMMAND ----------

def rule(name, dimension, func, column, criticality="error", **kwargs):
    return DQRowRule(
        name=name,
        criticality=criticality,
        check_func=func,
        column=column,
        check_func_kwargs=kwargs,
        user_metadata={"dimension": dimension},
    )


def weeks_rule():
    return rule(
        "weeks_count out of range", "validity", check_funcs.is_in_range, "weeks_count",
        min_limit=Q["weeks_per_month_min"], max_limit=Q["weeks_per_month_max"],
    )


SUITE = [
    ("silver", T["prices_silver"], [
        rule("price_sk is null", "completeness", check_funcs.is_not_null, "price_sk"),
        rule("series_bk is null", "completeness", check_funcs.is_not_null, "series_bk"),
        rule("effective_from is null", "completeness", check_funcs.is_not_null, "effective_from"),
        rule(f"price not in {Q['price_min']}..{Q['price_max']}", "validity",
             check_funcs.is_in_range, "price", min_limit=Q["price_min"], max_limit=Q["price_max"]),
    ]),
    ("silver", T["consumption_silver"], [
        rule("consumption_sk is null", "completeness", check_funcs.is_not_null, "consumption_sk"),
        rule("series_bk is null", "completeness", check_funcs.is_not_null, "series_bk"),
        rule("period_bk is null", "completeness", check_funcs.is_not_null, "period_bk"),
        rule("duoarea_bk is null", "completeness", check_funcs.is_not_null, "duoarea_bk"),
    ]),
    ("gold", T["dim_product"], [
        rule("price_product_code is null", "completeness", check_funcs.is_not_null,
             "price_product_code", criticality="warn"),
    ]),
    ("gold", T["fct_prices_monthly"], [weeks_rule()]),
    ("gold", T["fct_consumption_monthly"], [weeks_rule()]),
]

# COMMAND ----------

def short_reasons(col):
    return F.transform(col, lambda e: F.struct(e["name"].alias("rule"), e["message"].alias("message")))


results = []
quarantine_parts = []

for layer, table, rules in SUITE:
    short_name = table.split(".")[-1]
    df = spark.table(table)
    total = df.count()
    checked = dq.apply_checks(df, rules).cache()

    violations = {}
    for c in ("_errors", "_warnings"):
        for r in checked.select(F.explode(c).alias("e")).groupBy("e.name").count().collect():
            violations[r["name"]] = r["count"]

    for r in rules:
        n = violations.get(r.name, 0)
        severity = "error" if r.criticality == "error" else "warn"
        status = "PASSED" if n == 0 else ("FAILED" if severity == "error" else "WARN")
        results.append((
            RUN_ID, RUN_TS, layer, short_name, r.user_metadata["dimension"],
            r.name, total, n, status, severity,
        ))

    bad = checked.filter((F.size("_errors") > 0) | (F.size("_warnings") > 0))
    quarantine_parts.append(bad.select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(f"{layer}.{short_name}").alias("table_name"),
        F.lit(RUN_TS).cast("timestamp").alias("quarantined_at"),
        F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}).alias("record_json"),
        F.to_json(F.struct(short_reasons("_errors").alias("errors"),
                           short_reasons("_warnings").alias("warnings"))).alias("failed_checks"),
    ))

results_df = spark.createDataFrame(
    results,
    "run_id string, run_ts timestamp, layer string, table_name string, dimension string, "
    "test_name string, total_rows bigint, result bigint, status string, severity string",
)
quarantine_df = quarantine_parts[0]
for part in quarantine_parts[1:]:
    quarantine_df = quarantine_df.unionByName(part)

# COMMAND ----------

results_df.write.mode("append").saveAsTable(T["dq_test_results"])
quarantine_df.write.mode("append").saveAsTable(T["dq_quarantine"])

# COMMAND ----------

display(spark.table(T["dq_test_results"]).filter(F.col("run_id") == RUN_ID).orderBy("layer", "table_name"))

# COMMAND ----------

display(
    spark.table(T["dq_quarantine"]).filter(F.col("run_id") == RUN_ID)
    .groupBy("table_name").count()
)