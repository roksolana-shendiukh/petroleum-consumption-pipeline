import posixpath
import yaml

REQUIRED_KEYS = [
    "catalog", "bronze_schema", "silver_schema", "gold_schema",
    "bronze_volumes_path", "consumption", "prices",
]
DATASET_KEYS = ["source_file", "source_table", "target_table"]


def load_config(path: str, environment: str) -> dict:
    """Load one environment section from the YAML config and validate its keys."""
    with open(path) as f:
        full_config = yaml.safe_load(f)

    if environment not in full_config:
        raise KeyError(f"Environment '{environment}' not found in {path}. Available: {sorted(full_config)}")

    config = full_config[environment]
    validate_config(config, environment)
    return config


def validate_config(config: dict, environment: str = "?") -> None:
    missing = [k for k in REQUIRED_KEYS if k not in config]
    if missing:
        raise ValueError(f"[{environment}] Missing config keys: {missing}")

    for dataset in ("consumption", "prices"):
        missing_ds = [k for k in DATASET_KEYS if k not in config[dataset]]
        if missing_ds:
            raise ValueError(f"[{environment}] Missing keys in '{dataset}': {missing_ds}")


def build_table_names(config: dict) -> dict[str, str]:
    """Fully qualified table names used across the medallion layers."""
    c, b, s, g = config["catalog"], config["bronze_schema"], config["silver_schema"], config["gold_schema"]
    return {
        "consumption_bronze": f"{c}.{b}.{config['consumption']['source_table']}",
        "prices_bronze": f"{c}.{b}.{config['prices']['source_table']}",
        "product_price_mapping": f"{c}.{b}.product_price_mapping",
        "consumption_silver": f"{c}.{s}.{config['consumption']['target_table']}",
        "prices_silver": f"{c}.{s}.{config['prices']['target_table']}",
        "dim_date": f"{c}.{g}.dim_date",
        "dim_product": f"{c}.{g}.dim_product",
        "dim_area": f"{c}.{g}.dim_area",
        "fct_consumption_weekly": f"{c}.{g}.fct_snapshot_consumption_weekly",
        "fct_prices_weekly": f"{c}.{g}.fct_snapshot_prices_weekly",
        "fct_consumption_monthly": f"{c}.{g}.fct_snapshot_consumption_monthly",
        "fct_prices_monthly": f"{c}.{g}.fct_snapshot_prices_monthly",
    }


def volume_file_path(config: dict, file_name: str) -> str:
    """Join volume dir and file name, so a missing trailing slash can't break the path."""
    return posixpath.join(config["bronze_volumes_path"], file_name)
