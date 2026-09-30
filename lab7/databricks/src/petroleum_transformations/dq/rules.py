from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule


def make_rule(name, dimension, func, column, criticality="error", **kwargs):
    return DQRowRule(
        name=name,
        criticality=criticality,
        check_func=func,
        column=column,
        check_func_kwargs=kwargs,
        user_metadata={"dimension": dimension},
    )


def weeks_rule(limits):
    return make_rule(
        "weeks_count out of range",
        "validity",
        check_funcs.is_in_range,
        "weeks_count",
        min_limit=limits["weeks_per_month_min"],
        max_limit=limits["weeks_per_month_max"],
    )


def build_suite(tables, limits):
    return [
        ("silver", tables["prices_silver"], [
            make_rule("price_sk is null", "completeness", check_funcs.is_not_null, "price_sk"),
            make_rule("series_bk is null", "completeness", check_funcs.is_not_null, "series_bk"),
            make_rule("effective_from is null", "completeness", check_funcs.is_not_null, "effective_from"),
            make_rule(
                f"price not in {limits['price_min']}..{limits['price_max']}",
                "validity",
                check_funcs.is_in_range,
                "price",
                min_limit=limits["price_min"],
                max_limit=limits["price_max"],
            ),
        ]),
        ("silver", tables["consumption_silver"], [
            make_rule("consumption_sk is null", "completeness", check_funcs.is_not_null, "consumption_sk"),
            make_rule("series_bk is null", "completeness", check_funcs.is_not_null, "series_bk"),
            make_rule("period_bk is null", "completeness", check_funcs.is_not_null, "period_bk"),
            make_rule("duoarea_bk is null", "completeness", check_funcs.is_not_null, "duoarea_bk"),
        ]),
        ("gold", tables["dim_product"], [
            make_rule(
                "price_product_code is null",
                "completeness",
                check_funcs.is_not_null,
                "price_product_code",
                criticality="warn",
            ),
        ]),
        ("gold", tables["fct_prices_monthly"], [weeks_rule(limits)]),
        ("gold", tables["fct_consumption_monthly"], [weeks_rule(limits)]),
    ]


def build_uniqueness_specs(t):
    return [
        ("silver", t["prices_silver"], ["series_bk", "effective_from"], "error"),
        ("silver", t["consumption_silver"], ["series_bk", "duoarea_bk", "period_bk"], "error"),
        ("gold", t["dim_date"], ["full_date"], "error"),
        ("gold", t["dim_area"], ["area_code"], "error"),
        ("gold", t["dim_product"], ["consumption_product_code"], "error"),
        ("gold", t["fct_prices_weekly"], ["dim_date_key", "dim_product_key", "series_bk"], "error"),
        ("gold", t["fct_prices_weekly"], ["dim_date_key", "dim_product_key"], "warn"),
        ("gold", t["fct_consumption_weekly"], ["dim_date_key", "dim_product_key", "dim_area_key"], "error"),
    ]


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
            "name": "distinct keys lost bronze -> silver",
            "source": t["consumption_bronze"], "source_distinct": ["series", "duoarea", "period"],
            "target": t["consumption_silver"], "target_distinct": ["series_bk", "duoarea_bk", "period_bk"],
        },
        {
            "kind": "count", "layer": "silver",
            "name": "distinct keys lost bronze -> silver",
            "source": t["prices_bronze"], "source_distinct": ["series", "period"],
            "target": t["prices_silver"], "target_distinct": ["series_bk", "effective_from"],
        },
        {
            "kind": "count", "layer": "gold",
            "name": "rows lost silver -> gold weekly",
            "source": t["prices_silver"], "source_distinct": None,
            "target": t["fct_prices_weekly"], "target_distinct": None,
        },
        {
            "kind": "count", "layer": "gold",
            "name": "rows lost silver -> gold weekly",
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
