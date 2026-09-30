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


def _short_reasons(column: str):
    return F.transform(
        column, lambda e: F.struct(e["name"].alias("rule"), e["message"].alias("message"))
    )


def _violations_by_rule(checked: DataFrame) -> dict:
    counts = {}
    for column in ("_errors", "_warnings"):
        rows = checked.select(F.explode(column).alias("e")).groupBy("e.name").count().collect()
        for row in rows:
            counts[row["name"]] = row["count"]
    return counts


def _error_row(run_id, run_ts, layer, table_name, message):
    return {
        "run_id": run_id, "run_ts": run_ts, "layer": layer, "table_name": table_name,
        "dimension": "execution", "test_name": f"checks could not run: {message}"[:200],
        "total_rows": 0, "result": 1, "status": "FAILED", "severity": "error",
    }


def run_row_checks(dq_engine, df: DataFrame, table_name: str, layer: str, rules: list,
                   run_id: str, run_ts: datetime):
    total = df.count()
    checked = dq_engine.apply_checks(df, rules)
    violations = _violations_by_rule(checked)

    results = []
    for rule in rules:
        severity = "error" if rule.criticality == "error" else "warn"
        count = violations.get(rule.name, 0)
        results.append({
            "run_id": run_id, "run_ts": run_ts, "layer": layer, "table_name": table_name,
            "dimension": rule.user_metadata["dimension"], "test_name": rule.name,
            "total_rows": total, "result": count,
            "status": result_status(count, severity), "severity": severity,
        })

    bad = checked.filter((F.size("_errors") > 0) | (F.size("_warnings") > 0))
    quarantine_df = bad.select(
        F.lit(run_id).alias("run_id"),
        F.lit(f"{layer}.{table_name}").alias("table_name"),
        F.lit(run_ts).cast("timestamp").alias("quarantined_at"),
        F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}).alias("record_json"),
        F.to_json(F.struct(
            _short_reasons("_errors").alias("errors"),
            _short_reasons("_warnings").alias("warnings"),
        )).alias("failed_checks"),
    )
    return results, quarantine_df


def run_suite(spark: SparkSession, dq_engine, suite: list, run_id: str, run_ts: datetime):
    results = []
    quarantine_df = None

    for layer, table, rules in suite:
        short_name = table.split(".")[-1]
        try:
            table_results, table_quarantine = run_row_checks(
                dq_engine, spark.table(table), short_name, layer, rules, run_id, run_ts
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