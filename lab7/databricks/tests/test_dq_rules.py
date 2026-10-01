from pathlib import Path

import pytest
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

from petroleum_transformations.config import build_table_names, load_config
from petroleum_transformations.dq.rules import dimension_of, load_row_suite

ROOT = Path(__file__).resolve().parents[1]
TABLES = {"prices_silver": "cat.slv.prices"}
LIMITS = {"price_min": 0.01, "price_max": 20}


def engine():
    return DQEngine(WorkspaceClient())


def write_checks(tmp_path, checks, file_name="prices_silver.yml"):
    (tmp_path / file_name).write_text(yaml.safe_dump(checks))
    return str(tmp_path)


def good_check(**overrides):
    check = {
        "name": "price_sk is null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "price_sk"}},
    }
    check.update(overrides)
    return check


def in_range(name, max_limit):
    return good_check(
        name=name,
        check={
            "function": "is_in_range",
            "arguments": {"column": "price", "min_limit": 0.01, "max_limit": max_limit},
        },
    )


def test_load_row_suite_maps_file_name_to_full_table_name(tmp_path):
    directory = write_checks(tmp_path, [good_check()])

    suite = load_row_suite(engine(), directory, TABLES, LIMITS)

    assert suite[0][0] == "cat.slv.prices"
    assert suite[0][1][0]["name"] == "price_sk is null"


def test_load_row_suite_substitutes_thresholds_from_config(tmp_path):
    templated = good_check(
        name="price not in {{ price_min }}..{{ price_max }}",
        check={
            "function": "is_in_range",
            "arguments": {"column": "price", "min_limit": "{{ price_min }}", "max_limit": "{{ price_max }}"},
        },
    )

    suite = load_row_suite(engine(), write_checks(tmp_path, [templated]), TABLES, LIMITS)
    loaded = suite[0][1][0]

    assert loaded["name"] == "price not in 0.01..20"
    assert float(loaded["check"]["arguments"]["min_limit"]) == 0.01
    assert float(loaded["check"]["arguments"]["max_limit"]) == 20


def test_load_row_suite_rejects_file_for_unknown_table(tmp_path):
    directory = write_checks(tmp_path, [good_check()], file_name="nope.yml")

    with pytest.raises(ValueError, match="nope"):
        load_row_suite(engine(), directory, TABLES, LIMITS)


def test_load_row_suite_requires_a_name(tmp_path):
    unnamed = good_check()
    del unnamed["name"]

    with pytest.raises(ValueError, match="name"):
        load_row_suite(engine(), write_checks(tmp_path, [unnamed]), TABLES, LIMITS)


def test_load_row_suite_rejects_duplicate_names(tmp_path):
    with pytest.raises(ValueError, match="unique"):
        load_row_suite(engine(), write_checks(tmp_path, [good_check(), good_check()]), TABLES, LIMITS)


def test_load_row_suite_rejects_unknown_check_function(tmp_path):
    bad = good_check(check={"function": "no_such_function", "arguments": {"column": "x"}})

    with pytest.raises(ValueError, match="invalid"):
        load_row_suite(engine(), write_checks(tmp_path, [bad]), TABLES, LIMITS)


def test_load_row_suite_rejects_semantically_duplicated_rules(tmp_path):
    checks = [good_check(name="price_sk is null"), good_check(name="price_sk is missing")]

    with pytest.raises(ValueError):
        load_row_suite(engine(), write_checks(tmp_path, checks), TABLES, LIMITS)


def test_load_row_suite_rejects_conflicting_thresholds(tmp_path):
    checks = [in_range("price low", 20), in_range("price high", 50)]

    with pytest.raises(ValueError):
        load_row_suite(engine(), write_checks(tmp_path, checks), TABLES, LIMITS)


def test_load_row_suite_rejects_an_empty_directory(tmp_path):
    with pytest.raises(ValueError, match="No DQ check files"):
        load_row_suite(engine(), str(tmp_path), TABLES, LIMITS)


def test_dimension_is_derived_from_the_check_function():
    assert dimension_of(good_check()) == "completeness"
    assert dimension_of(in_range("r", 20)) == "validity"
    unique = good_check(check={"function": "is_unique", "arguments": {"columns": ["a", "b"]}})
    assert dimension_of(unique) == "uniqueness"


def test_dimension_can_be_overridden_with_user_metadata():
    check = good_check(user_metadata={"dimension": "consistency"})

    assert dimension_of(check) == "consistency"


def test_dimension_of_an_unmapped_function_raises():
    unmapped = good_check(check={"function": "sql_expression", "arguments": {"expression": "a > 0"}})

    with pytest.raises(ValueError, match="sql_expression"):
        dimension_of(unmapped)


def test_real_check_files_are_valid_for_every_environment():
    for environment in ("dev", "prod"):
        cfg = load_config(str(ROOT / "config" / "pipeline_config.yaml"), environment)
        suite = load_row_suite(
            engine(), str(ROOT / "config" / "dq_checks"), build_table_names(cfg), cfg["dq"]
        )

        assert len(suite) == 11
        for _, checks in suite:
            assert checks
            assert len({check["name"] for check in checks}) == len(checks)
            assert all(dimension_of(check) for check in checks)


def test_sql_expression_rules_declare_columns_and_dimension():
    cfg = load_config(str(ROOT / "config" / "pipeline_config.yaml"), "dev")
    suite = load_row_suite(
        engine(), str(ROOT / "config" / "dq_checks"), build_table_names(cfg), cfg["dq"]
    )

    found = [c for _, checks in suite for c in checks if c["check"]["function"] == "sql_expression"]

    assert found
    for check in found:
        assert check["check"]["arguments"].get("columns")
        assert (check.get("user_metadata") or {}).get("dimension")