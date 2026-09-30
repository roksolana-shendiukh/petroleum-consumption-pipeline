from databricks.labs.dqx.config import TableChecksStorageConfig


def layer_of(table, cfg):
    schema = table.split(".")[1]
    for layer in ("bronze", "silver", "gold"):
        if cfg[f"{layer}_schema"] == schema:
            return layer
    raise ValueError(f"Cannot determine layer of {table}: schema '{schema}' is not in the config")


def deploy_checks(dq_engine, suite, checks_table):
    for table, checks in suite:
        dq_engine.save_checks(
            checks,
            config=TableChecksStorageConfig(
                location=checks_table, run_config_name=table, mode="overwrite"
            ),
        )


def load_suite(spark, dq_engine, checks_table, cfg):
    names = sorted(
        row["run_config_name"]
        for row in spark.table(checks_table).select("run_config_name").distinct().collect()
    )
    suite = []
    for table in names:
        checks = dq_engine.load_checks(
            config=TableChecksStorageConfig(location=checks_table, run_config_name=table)
        )
        status = dq_engine.validate_checks(checks)
        if status.has_errors:
            raise ValueError(f"[{table}] invalid DQX checks in {checks_table}: {status}")
        suite.append((layer_of(table, cfg), table, checks))
    return suite