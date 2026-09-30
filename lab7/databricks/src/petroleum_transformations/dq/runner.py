from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RESULT_COLUMNS = [
    "run_id", "run_ts", "layer", "table_name", "dimension",
    "test_name", "total_rows", "result", "status", "severity",
]
RESULTS_SCHEMA = (
    "run_id string, run_ts timestamp, layer string, table_name string, dimension string, "
    "test_name string, total_rows bigint, result bigint, status string, severity string"
)
QUARANTINE_SCHEMA = (
    "run_id string, table_name string, quarantined_at timestamp, record_json string, failed_checks string"
)
TRACE_FIELDS = ("rule_fingerprint", "rule_set_fingerprint")


def create_dq_tables(spark: SparkSession, tables: dict) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tables['dq_test_results']} ({RESULTS_SCHEMA}) USING DELTA "
        "COMMENT 'One row per executed data quality check'"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tables['dq_quarantine']} ({QUARANTINE_SCHEMA}) USING DELTA "
        "COMMENT 'Bad records from all tables in one place'"
    )


def result_status(violations: int, severity: str) -> str:
    if violations == 0:
        return "PASSED"
    return "FAILED" if severity == "error" else "WARN"


def _severity(check: dict) -> str:
    return "error" if check.get("criticality", "error") == "error" else "warn"


def _trace_fields(checked: DataFrame) -> list:
    element = checked.schema["_errors"].dataType.elementType
    present = set(element.fieldNames())
    return [field for field in TRACE_FIELDS if field in present]


def _short_reasons(column: str, trace_fields: list):
    return F.transform(
        column,
        lambda e: F.struct(
            e["name"].alias("rule"),
            e["message"].alias("message"),
            *[e[field].alias(field) for field in trace_fields],
        ),
    )


def _has_rule(column: str, name: str):
    return F.coalesce(F.exists(column, lambda e: e["name"] == name), F.lit(False))


def _is_bad_row():
    return (F.coalesce(F.size("_errors"), F.lit(0)) > 0) | (F.coalesce(F.size("_warnings"), F.lit(0)) > 0)


def _referenced_columns(check: dict) -> list:
    definition = check["check"]
    arguments = definition.get("arguments") or {}
    names = []
    if "column" in arguments:
        names.append(arguments["column"])
    names.extend(arguments.get("columns") or [])
    names.extend(definition.get("for_each_column") or [])
    return [name for name in names if isinstance(name, str)]


def missing_columns(df: DataFrame, checks: list) -> list:
    available = {column.lower() for column in df.columns}
    missing = []
    for check in checks:
        for name in _referenced_columns(check):
            if "(" in name or " " in name:
                continue
            if name.split(".")[0].lower() not in available:
                missing.append((check["name"], name))
    return missing


def _error_row(run_id, run_ts, layer, table_name, message):
    return {
        "run_id": run_id, "run_ts": run_ts, "layer": layer, "table_name": table_name,
        "dimension": "execution", "test_name": f"checks could not run: {message}"[:200],
        "total_rows": 0, "result": 1, "status": "FAILED", "severity": "error",
    }


def run_row_checks(dq_engine, df: DataFrame, table_name: str, layer: str, checks: list,
                   run_id: str, run_ts: datetime):
    missing = missing_columns(df, checks)
    if missing:
        details = ", ".join(f"{rule}: {column}" for rule, column in missing)
        raise ValueError(f"columns not found in {table_name}: {details}")

    checked = dq_engine.apply_checks_by_metadata(df, checks)
    trace_fields = _trace_fields(checked)

    aggregates = [F.count(F.lit(1)).alias("total")]
    for i, check in enumerate(checks):
        hit = _has_rule("_errors", check["name"]) | _has_rule("_warnings", check["name"])
        aggregates.append(F.sum(F.when(hit, 1).otherwise(0)).alias(f"v{i}"))
    metrics = checked.agg(*aggregates).collect()[0]

    total = metrics["total"]
    results = []
    any_violation = False
    for i, check in enumerate(checks):
        severity = _severity(check)
        count = metrics[f"v{i}"] or 0
        any_violation = any_violation or count > 0
        results.append({
            "run_id": run_id, "run_ts": run_ts, "layer": layer, "table_name": table_name,
            "dimension": check["user_metadata"]["dimension"], "test_name": check["name"],
            "total_rows": total, "result": count,
            "status": result_status(count, severity), "severity": severity,
        })

    if not any_violation:
        return results, df.sparkSession.createDataFrame([], QUARANTINE_SCHEMA)

    quarantine_df = checked.filter(_is_bad_row()).select(
        F.lit(run_id).alias("run_id"),
        F.lit(f"{layer}.{table_name}").alias("table_name"),
        F.lit(run_ts).cast("timestamp").alias("quarantined_at"),
        F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}).alias("record_json"),
        F.to_json(F.struct(
            _short_reasons("_errors", trace_fields).alias("errors"),
            _short_reasons("_warnings", trace_fields).alias("warnings"),
        )).alias("failed_checks"),
    )
    return results, quarantine_df


def run_suite(spark: SparkSession, dq_engine, suite: list, run_id: str, run_ts: datetime):
    results = []
    quarantine_df = None

    for layer, table, checks in suite:
        short_name = table.split(".")[-1]
        try:
            table_results, table_quarantine = run_row_checks(
                dq_engine, spark.table(table), short_name, layer, checks, run_id, run_ts
            )
        except Exception as e:
            results.append(_error_row(run_id, run_ts, layer, short_name, str(e).splitlines()[0]))
            continue
        results.extend(table_results)
        quarantine_df = (
            table_quarantine if quarantine_df is None else quarantine_df.unionByName(table_quarantine)
        )

    rows = [tuple(r[c] for c in RESULT_COLUMNS) for r in results]
    results_df = spark.createDataFrame(rows, RESULTS_SCHEMA)
    if quarantine_df is None:
        quarantine_df = spark.createDataFrame([], QUARANTINE_SCHEMA)
    return results_df, quarantine_df


def write_dq_outputs(results_df: DataFrame, quarantine_df: DataFrame, tables: dict) -> None:
    results_df.write.mode("append").saveAsTable(tables["dq_test_results"])
    quarantine_df.write.mode("append").saveAsTable(tables["dq_quarantine"])


def blocking_failures(results_df: DataFrame) -> list:
    return (
        results_df.filter((F.col("status") == "FAILED") & (F.col("severity") == "error")).collect()
    )