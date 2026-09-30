import re

import yaml
from databricks.labs.dqx.checks_semantic_validator import ChecksSemanticValidationMode
from databricks.labs.dqx.engine import DQEngine

_VARIABLE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def _lookup(variables, name):
    if name not in variables:
        raise ValueError(f"Unknown variable '{name}' in DQ checks file")
    return variables[name]


def substitute(value, variables):
    if isinstance(value, dict):
        return {k: substitute(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, variables) for v in value]
    if isinstance(value, str):
        whole = _VARIABLE.fullmatch(value.strip())
        if whole:
            return _lookup(variables, whole.group(1))
        return _VARIABLE.sub(lambda m: str(_lookup(variables, m.group(1))), value)
    return value


def validate_row_checks(checks, table_key):
    names = [c.get("name") for c in checks]
    if not all(names):
        raise ValueError(f"[{table_key}] every check needs a name")
    if len(set(names)) != len(names):
        raise ValueError(f"[{table_key}] check names must be unique")
    for c in checks:
        if "dimension" not in (c.get("user_metadata") or {}):
            raise ValueError(f"[{table_key}] check '{c['name']}' needs user_metadata.dimension")
    status = DQEngine.validate_checks(
        checks, semantic_validation_mode=ChecksSemanticValidationMode.FAIL
    )
    if status.has_errors:
        raise ValueError(f"[{table_key}] invalid DQX checks: {status.errors}")


def load_row_suite(path, tables, limits):
    with open(path) as f:
        entries = yaml.safe_load(f)

    suite = []
    for entry in entries:
        key = entry["table"]
        if key not in tables:
            raise ValueError(f"Unknown table '{key}' in DQ checks file")
        checks = substitute(entry["checks"], limits)
        validate_row_checks(checks, key)
        suite.append((entry["layer"], tables[key], checks))
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