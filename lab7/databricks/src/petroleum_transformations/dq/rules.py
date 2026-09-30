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