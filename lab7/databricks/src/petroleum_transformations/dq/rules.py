from pathlib import Path

from databricks.labs.dqx.checks_semantic_validator import ChecksSemanticValidationMode
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine

FUNCTION_DIMENSIONS = {
    "is_not_null": "completeness",
    "is_not_null_and_not_empty": "completeness",
    "is_in_range": "validity",
    "is_not_less_than": "validity",
    "is_not_greater_than": "validity",
    "is_in_list": "validity",
    "is_valid_date": "validity",
    "regex_match": "validity",
    "is_unique": "uniqueness",
}


def dimension_of(check):
    override = (check.get("user_metadata") or {}).get("dimension")
    if override:
        return override
    function = check["check"]["function"]
    if function not in FUNCTION_DIMENSIONS:
        raise ValueError(
            f"Cannot derive a quality dimension for '{function}': "
            "add it to FUNCTION_DIMENSIONS or set user_metadata.dimension"
        )
    return FUNCTION_DIMENSIONS[function]


def validate_row_checks(checks, table_key):
    names = [check.get("name") for check in checks]
    if not all(names):
        raise ValueError(f"[{table_key}] every check needs a name")
    if len(set(names)) != len(names):
        raise ValueError(f"[{table_key}] check names must be unique")

    status = DQEngine.validate_checks(
        checks, semantic_validation_mode=ChecksSemanticValidationMode.FAIL
    )
    if status.has_errors:
        raise ValueError(f"[{table_key}] invalid DQX checks: {status}")

    for check in checks:
        dimension_of(check)


def load_row_suite(dq_engine, directory, tables, limits):
    suite = []
    for path in sorted(Path(directory).glob("*.yml")):
        key = path.stem
        if key not in tables:
            raise ValueError(f"Unknown table '{key}' (file {path.name})")
        checks = dq_engine.load_checks(FileChecksStorageConfig(location=str(path)), variables=limits)
        validate_row_checks(checks, key)
        suite.append((tables[key], checks))
    if not suite:
        raise ValueError(f"No DQ check files found in {directory}")
    return suite


def build_reference_specs(t):
    return [
        ("silver", t["prices_silver"], "effective_from", t["dim_date"], "full_date", "error"),
        ("silver", t["prices_silver"], "product_bk", t["dim_product"], "price_product_code", "error"),
        ("silver", t["consumption_silver"], "period_bk", t["dim_date"], "full_date", "error"),
        ("silver", t["consumption_silver"], "product_bk", t["dim_product"], "consumption_product_code", "error"),
        ("silver", t["consumption_silver"], "duoarea_bk", t["dim_area"], "area_code", "error"),
        ("gold", t["fct_prices_weekly"], "dim_date_key", t["dim_date"], "date_key", "error"),
        ("gold", t["fct_prices_weekly"], "dim_product_key", t["dim_product"], "product_key", "error"),
        ("gold", t["fct_consumption_weekly"], "dim_date_key", t["dim_date"], "date_key", "error"),
        ("gold", t["fct_consumption_weekly"], "dim_product_key", t["dim_product"], "product_key", "error"),
        ("gold", t["fct_consumption_weekly"], "dim_area_key", t["dim_area"], "area_key", "error"),
        ("gold", t["fct_consumption_monthly"], "dim_product_key", t["dim_product"], "product_key", "error"),
        ("gold", t["fct_prices_monthly"], "dim_product_key", t["dim_product"], "product_key", "error"),
    ]


def build_coverage_specs(t):
    return [
        ("gold", t["dim_date"], "full_date", t["prices_silver"], "effective_from", "error"),
        ("gold", t["dim_date"], "full_date", t["consumption_silver"], "period_bk", "error"),
    ]


def build_age_specs(t):
    return [
        ("silver", t["prices_silver"], "effective_from", "warn"),
        ("silver", t["consumption_silver"], "period_bk", "warn"),
    ]


def build_reconciliation_specs(t):
    return [
        {
            "kind": "count", "layer": "silver",
            "name": "distinct keys differ bronze -> silver",
            "source": t["consumption_bronze"], "source_distinct": ["series", "duoarea", "period"],
            "target": t["consumption_silver"], "target_distinct": ["series_bk", "duoarea_bk", "period_bk"],
        },
        {
            "kind": "count", "layer": "silver",
            "name": "distinct keys differ bronze -> silver",
            "source": t["prices_bronze"], "source_distinct": ["series", "period"],
            "target": t["prices_silver"], "target_distinct": ["series_bk", "effective_from"],
        },
        {
            "kind": "count", "layer": "gold",
            "name": "row count differs silver -> gold weekly",
            "source": t["prices_silver"], "source_distinct": None,
            "target": t["fct_prices_weekly"], "target_distinct": None,
        },
        {
            "kind": "count", "layer": "gold",
            "name": "row count differs silver -> gold weekly",
            "source": t["consumption_silver"], "source_distinct": None,
            "target": t["fct_consumption_weekly"], "target_distinct": None,
        },
        {
            "kind": "sum", "layer": "gold",
            "name": "sum of consumption_value differs silver -> gold weekly",
            "source": t["consumption_silver"], "source_col": "consumption_value",
            "target": t["fct_consumption_weekly"], "target_col": "consumption_value",
        },
    ]