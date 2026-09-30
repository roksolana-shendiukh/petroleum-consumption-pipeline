from pathlib import Path

import pytest
import yaml

from petroleum_transformations.config import build_table_names, load_config
from petroleum_transformations.dq.rules import load_row_suite, substitute

ROOT = Path(__file__).resolve().parents[1]

TABLES = {"prices_silver": "cat.slv.prices"}
LIMITS = {"price_min": 0.01, "price_max": 20}


def write_checks(tmp_path, entries):
    path = tmp_path / "dq_checks.yml"
    path.write_text(yaml.safe_dump(entries))
    return str(path)


def entry(checks):
    return [{"table": "prices_silver", "layer": "silver", "checks": checks}]


def good_check(**overrides):
    check = {
        "name": "price_sk is null",
        "criticality": "error",
        "user_metadata": {"dimension": "completeness"},
        "check": {"function": "is_not_null", "arguments": {"column": "price_sk"}},
    }
    check.update(overrides)
    return check


def test_substitute_keeps_number_type_when_whole_value_is_a_variable():
    assert substitute("{{ price_min }}", LIMITS) == 0.01
    assert substitute({"a": ["{{ price_max }}"]}, LIMITS) == {"a": [20]}


def test_substitute_inserts_text_inside_a_longer_string():
    assert substitute("price not in {{ price_min }}..{{ price_max }}", LIMITS) == "price not in 0.01..20"


def test_substitute_unknown_variable_raises():
    with pytest.raises(ValueError, match="nope"):
        substitute("{{ nope }}", LIMITS)


def test_load_row_suite_maps_logical_table_to_full_name(tmp_path):
    path = write_checks(tmp_path, entry([good_check()]))

    suite = load_row_suite(path, TABLES, LIMITS)

    assert suite[0][0] == "silver"
    assert suite[0][1] == "cat.slv.prices"
    assert suite[0][2][0]["name"] == "price_sk is null"


def test_load_row_suite_rejects_unknown_table(tmp_path):
    path = write_checks(tmp_path, [{"table": "nope", "layer": "silver", "checks": [good_check()]}])

    with pytest.raises(ValueError, match="nope"):
        load_row_suite(path, TABLES, LIMITS)


def test_load_row_suite_requires_name_and_dimension(tmp_path):
    no_name = good_check()
    del no_name["name"]
    no_dimension = good_check(user_metadata={})

    with pytest.raises(ValueError, match="name"):
        load_row_suite(write_checks(tmp_path, entry([no_name])), TABLES, LIMITS)
    with pytest.raises(ValueError, match="dimension"):
        load_row_suite(write_checks(tmp_path, entry([no_dimension])), TABLES, LIMITS)


def test_load_row_suite_rejects_duplicate_names(tmp_path):
    path = write_checks(tmp_path, entry([good_check(), good_check()]))

    with pytest.raises(ValueError, match="unique"):
        load_row_suite(path, TABLES, LIMITS)


def test_load_row_suite_rejects_unknown_check_function(tmp_path):
    bad = good_check(check={"function": "no_such_function", "arguments": {"column": "x"}})

    with pytest.raises(ValueError, match="invalid"):
        load_row_suite(write_checks(tmp_path, entry([bad])), TABLES, LIMITS)


def test_real_checks_file_is_valid_for_every_environment():
    for environment in ("dev", "prod"):
        cfg = load_config(str(ROOT / "config" / "pipeline_config.yaml"), environment)
        suite = load_row_suite(str(ROOT / "config" / "dq_checks.yml"), build_table_names(cfg), cfg["dq"])

        assert len(suite) == 9
        assert all(checks for _, _, checks in suite)

def test_load_row_suite_rejects_semantically_duplicated_rules(tmp_path):
    first = good_check(name="price_sk is null")
    second = good_check(name="price_sk is missing")

    with pytest.raises(ValueError):
        load_row_suite(write_checks(tmp_path, entry([first, second])), TABLES, LIMITS)


def test_load_row_suite_rejects_conflicting_thresholds(tmp_path):
    def in_range(name, max_limit):
        return good_check(
            name=name,
            check={
                "function": "is_in_range",
                "arguments": {"column": "price", "min_limit": 0.01, "max_limit": max_limit},
            },
        )

    with pytest.raises(ValueError):
        load_row_suite(
            write_checks(tmp_path, entry([in_range("price low", 20), in_range("price high", 50)])),
            TABLES,
            LIMITS,
        )