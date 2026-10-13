import json
from typing import Any

REQUIRED_KEYS = (
    "endpoint_name",
    "endpoint_type",
    "index_name",
    "primary_key",
    "embedding_vector_column",
    "pipeline_type",
    "columns_to_sync",
    "timeout_s",
)


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if not config.get(key):
            raise ValueError(f"{key} is required")
    if not isinstance(config["timeout_s"], int) or config["timeout_s"] <= 0:
        raise ValueError("timeout_s must be a positive integer")
    return config


def full_index_name(catalog: str, schema: str, settings: dict[str, Any]) -> str:
    return f"{catalog}.{schema}.{settings['index_name']}"