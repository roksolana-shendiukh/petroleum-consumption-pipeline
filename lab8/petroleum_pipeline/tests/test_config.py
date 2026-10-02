import pytest
import yaml

from petroleum_transformations.config import (
    build_table_names,
    load_config,
    validate_config,
    volume_file_path,
)

VALID = {
    "dev": {
        "catalog": "cat_dev",
        "bronze_schema": "brz",
        "silver_schema": "slv",
        "gold_schema": "gld",
        "bronze_volumes_path": "/Volumes/cat_dev/x/raw_files/",
        "consumption": {"source_file": "c.json", "source_table": "c_raw", "target_table": "c_silver"},
        "prices": {"source_file": "p.json", "source_table": "p_raw", "target_table": "p_silver"},
    },
    "prod": {
        "catalog": "cat_prod",
        "bronze_schema": "brz",
        "silver_schema": "slv",
        "gold_schema": "gld",
        "bronze_volumes_path": "/Volumes/cat_prod/x/raw_files/",
        "consumption": {"source_file": "c.json", "source_table": "c_raw", "target_table": "c_silver"},
        "prices": {"source_file": "p.json", "source_table": "p_raw", "target_table": "p_silver"},
    },
}


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(VALID))
    return str(path)


def test_load_config_returns_selected_environment(config_file):
    assert load_config(config_file, "dev")["catalog"] == "cat_dev"
    assert load_config(config_file, "prod")["catalog"] == "cat_prod"


def test_load_config_unknown_environment_raises(config_file):
    with pytest.raises(KeyError, match="staging"):
        load_config(config_file, "staging")


def test_load_config_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(str(tmp_path / "nope.yaml"), "dev")


def test_validate_config_reports_missing_top_level_keys():
    bad = {k: v for k, v in VALID["dev"].items() if k not in ("catalog", "gold_schema")}

    with pytest.raises(ValueError, match="catalog") as exc:
        validate_config(bad, "dev")
    assert "gold_schema" in str(exc.value)


def test_validate_config_reports_missing_dataset_keys():
    bad = {**VALID["dev"], "prices": {"source_file": "p.json"}}

    with pytest.raises(ValueError, match="prices"):
        validate_config(bad, "dev")


def test_build_table_names_dev_and_prod_differ_only_by_catalog():
    dev = build_table_names(VALID["dev"])
    prod = build_table_names(VALID["prod"])

    assert dev["consumption_silver"] == "cat_dev.slv.c_silver"
    assert dev["fct_prices_monthly"] == "cat_dev.gld.fct_snapshot_prices_monthly"
    assert dev["product_price_mapping"] == "cat_dev.brz.product_price_mapping"
    assert {k: v.replace("cat_dev", "cat_prod") for k, v in dev.items()} == prod


def test_volume_file_path_handles_trailing_slash():
    with_slash = {"bronze_volumes_path": "/Volumes/c/s/raw/"}
    without_slash = {"bronze_volumes_path": "/Volumes/c/s/raw"}

    assert volume_file_path(with_slash, "a.json") == "/Volumes/c/s/raw/a.json"
    assert volume_file_path(without_slash, "a.json") == "/Volumes/c/s/raw/a.json"