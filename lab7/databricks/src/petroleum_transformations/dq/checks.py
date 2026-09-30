from datetime import datetime
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from petroleum_transformations.dq.rules import (
    build_age_specs,
    build_coverage_specs,
    build_reconciliation_specs,
    build_reference_specs,
    build_uniqueness_specs,
)
from petroleum_transformations.dq.runner import (
    QUARANTINE_SCHEMA,
    RESULT_COLUMNS,
    RESULTS_SCHEMA,
    result_status,
)

REASON_TYPE = "array<struct<rule:string,message:string>>"


class RunContext(NamedTuple):
    run_id: str
    run_ts: datetime


def _short(table):
    return table.split(".")[-1]


def _row(ctx, layer, table_name, dimension, test_name, total, count, severity, status):
    return {
        "run_id": ctx.run_id, "run_ts": ctx.run_ts, "layer": layer, "table_name": table_name,
        "dimension": dimension, "test_name": test_name,
        "total_rows": int(total), "result": int(count), "status": status, "severity": severity,
    }


def _failed_to_run(ctx, layer, table_name, description, message):
    return _row(
        ctx, layer, table_name, "execution",
        f"{description}: could not run: {message}"[:200], 0, 1, "error", "FAILED",
    )


def _quarantine(ctx, layer, table_name, bad, columns, rule, message, severity):
    reason = F.array(F.struct(F.lit(rule).alias("rule"), F.lit(message).alias("message")))
    empty = F.array().cast(REASON_TYPE)
    errors, warnings = (reason, empty) if severity == "error" else (empty, reason)
    return bad.select(
        F.lit(ctx.run_id).alias("run_id"),
        F.lit(f"{layer}.{table_name}").alias("table_name"),
        F.lit(ctx.run_ts).cast("timestamp").alias("quarantined_at"),
        F.to_json(F.struct(*columns), {"ignoreNullFields": "false"}).alias("record_json"),
        F.to_json(F.struct(errors.alias("errors"), warnings.alias("warnings"))).alias("failed_checks"),
    )


def _tolerance_status(diff, pct, max_pct):
    if diff == 0:
        return "PASSED"
    return "WARN" if pct <= max_pct else "FAILED"


def check_uniqueness(ctx, layer, table_name, df, keys, severity):
    keyed = df.dropna(subset=keys)
    groups = keyed.groupBy(*keys).count()
    metrics = groups.agg(
        F.sum("count").alias("total"),
        F.sum(F.when(F.col("count") > 1, F.col("count") - 1).otherwise(0)).alias("extra"),
    ).collect()[0]
    total = metrics["total"] or 0
    extra = metrics["extra"] or 0
    test_name = f"duplicate key ({', '.join(keys)})"
    row = _row(ctx, layer, table_name, "uniqueness", test_name, total, extra, severity,
               result_status(extra, severity))
    if extra == 0:
        return row, None
    duplicated = groups.filter(F.col("count") > 1).select(*keys)
    bad = keyed.join(duplicated, keys, "left_semi")
    return row, _quarantine(ctx, layer, table_name, bad, df.columns, test_name,
                            "key appears more than once", severity)


def check_reference(ctx, layer, table_name, child, child_col, parent, parent_col, parent_name, severity):
    keyed = child.filter(F.col(child_col).isNotNull())
    lookup = parent.select(F.col(parent_col).alias("_parent_key")).distinct()
    condition = F.col(child_col) == F.col("_parent_key")
    metrics = (
        keyed.join(lookup.withColumn("_hit", F.lit(1)), condition, "left")
        .agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("_hit").isNull(), 1).otherwise(0)).alias("orphans"),
        )
        .collect()[0]
    )
    total = metrics["total"] or 0
    orphans = metrics["orphans"] or 0
    test_name = f"{child_col} has no match in {parent_name}.{parent_col}"
    row = _row(ctx, layer, table_name, "consistency", test_name, total, orphans, severity,
               result_status(orphans, severity))
    if orphans == 0:
        return row, None
    bad = keyed.join(lookup, condition, "left_anti")
    return row, _quarantine(ctx, layer, table_name, bad, child.columns, test_name,
                            f"no matching {parent_col} in {parent_name}", severity)


def check_coverage(ctx, layer, table_name, parent, parent_col, child, child_col, child_name,
                   allowed_lag_days, severity):
    child_metrics = child.agg(F.max(child_col).alias("m"), F.count(F.lit(1)).alias("n")).collect()[0]
    parent_max = parent.agg(F.max(parent_col)).collect()[0][0]
    child_max = child_metrics["m"]
    if child_max is None:
        lag = 0
    elif parent_max is None:
        lag = allowed_lag_days + 1
    else:
        lag = (child_max - parent_max).days
    over = max(lag - allowed_lag_days, 0)
    test_name = f"{parent_col} covers latest {child_name}.{child_col}"
    return _row(ctx, layer, table_name, "timeliness", test_name, child_metrics["n"], over, severity,
                result_status(over, severity)), None


def check_age(ctx, layer, table_name, df, column, allowed_days, severity):
    metrics = df.agg(
        F.max(column).alias("m"),
        F.count(F.lit(1)).alias("n"),
        F.datediff(F.current_date(), F.max(column)).alias("age"),
    ).collect()[0]
    over = 1 if metrics["m"] is None else max((metrics["age"] or 0) - allowed_days, 0)
    test_name = f"{column} is at most {allowed_days} days old"
    return _row(ctx, layer, table_name, "timeliness", test_name, metrics["n"], over, severity,
                result_status(over, severity)), None


def check_count_reconciliation(ctx, layer, table_name, name, source, target, max_diff_pct):
    source_rows = source.count()
    target_rows = target.count()
    diff = abs(source_rows - target_rows)
    pct = 100.0 * diff / source_rows if source_rows else 0.0
    return _row(ctx, layer, table_name, "consistency", name, source_rows, diff, "error",
                _tolerance_status(diff, pct, max_diff_pct)), None


def check_sum_reconciliation(ctx, layer, table_name, name, source, source_col, target, target_col, max_diff_pct):
    source_metrics = source.agg(F.sum(source_col).alias("s"), F.count(F.lit(1)).alias("n")).collect()[0]
    target_sum = float(target.agg(F.sum(target_col)).collect()[0][0] or 0)
    source_sum = float(source_metrics["s"] or 0)
    diff = abs(source_sum - target_sum)
    pct = 100.0 * diff / abs(source_sum) if source_sum else 0.0
    lost = int(round(diff))
    return _row(ctx, layer, table_name, "consistency", name, source_metrics["n"], lost, "error",
                _tolerance_status(lost, pct, max_diff_pct)), None


def _side(spark, table, distinct):
    df = spark.table(table)
    return df.select(*distinct).distinct() if distinct else df


def run_table_checks(spark: SparkSession, tables: dict, limits: dict, ctx: RunContext):
    results = []
    quarantines = []

    def guarded(layer, table_name, description, task):
        try:
            row, quarantine = task()
        except Exception as e:
            results.append(_failed_to_run(ctx, layer, table_name, description, str(e).splitlines()[0]))
            return
        results.append(row)
        if quarantine is not None:
            quarantines.append(quarantine)

    for layer, table, keys, severity in build_uniqueness_specs(tables):
        name = _short(table)
        guarded(layer, name, f"duplicate key ({', '.join(keys)})",
                lambda t=table, k=keys, s=severity, l=layer, n=name:
                check_uniqueness(ctx, l, n, spark.table(t), k, s))

    for layer, child, child_col, parent, parent_col, severity in build_reference_specs(tables):
        name = _short(child)
        guarded(layer, name, f"{child_col} -> {_short(parent)}.{parent_col}",
                lambda c=child, cc=child_col, p=parent, pc=parent_col, s=severity, l=layer, n=name:
                check_reference(ctx, l, n, spark.table(c), cc, spark.table(p), pc, _short(p), s))

    for layer, parent, parent_col, child, child_col, severity in build_coverage_specs(tables):
        name = _short(parent)
        guarded(layer, name, f"{parent_col} covers {_short(child)}.{child_col}",
                lambda p=parent, pc=parent_col, c=child, cc=child_col, s=severity, l=layer, n=name:
                check_coverage(ctx, l, n, spark.table(p), pc, spark.table(c), cc, _short(c),
                               limits["dim_date_max_lag_days"], s))

    for layer, table, column, severity in build_age_specs(tables):
        name = _short(table)
        guarded(layer, name, f"{column} age",
                lambda t=table, c=column, s=severity, l=layer, n=name:
                check_age(ctx, l, n, spark.table(t), c, limits["max_age_days"], s))

    for spec in build_reconciliation_specs(tables):
        name = _short(spec["target"])
        if spec["kind"] == "count":
            guarded(spec["layer"], name, spec["name"],
                    lambda sp=spec, n=name: check_count_reconciliation(
                        ctx, sp["layer"], n, f"{sp['name']} ({_short(sp['source'])})",
                        _side(spark, sp["source"], sp["source_distinct"]),
                        _side(spark, sp["target"], sp["target_distinct"]),
                        limits["reconciliation_max_loss_pct"]))
        else:
            guarded(spec["layer"], name, spec["name"],
                    lambda sp=spec, n=name: check_sum_reconciliation(
                        ctx, sp["layer"], n, sp["name"],
                        spark.table(sp["source"]), sp["source_col"],
                        spark.table(sp["target"]), sp["target_col"],
                        limits["reconciliation_max_loss_pct"]))

    rows = [tuple(r[c] for c in RESULT_COLUMNS) for r in results]
    results_df = spark.createDataFrame(rows, RESULTS_SCHEMA)
    quarantine_df = None
    for part in quarantines:
        quarantine_df = part if quarantine_df is None else quarantine_df.unionByName(part)
    if quarantine_df is None:
        quarantine_df = spark.createDataFrame([], QUARANTINE_SCHEMA)
    return results_df, quarantine_df