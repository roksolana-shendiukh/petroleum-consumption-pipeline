import json
from typing import Any

REQUIRED_KEYS = ("top_k", "query_type", "query_types", "columns")
EVALUATION_KEYS = ("k", "query_types")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if not config.get(key):
            raise ValueError(f"{key} is required")
    if not isinstance(config["top_k"], int) or config["top_k"] <= 0:
        raise ValueError("top_k must be a positive integer")
    if config["query_type"] not in config["query_types"]:
        raise ValueError(f"query_type must be one of {config['query_types']}")
    return config


def load_evaluation_config(path: str, retrieval: dict[str, Any]) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in EVALUATION_KEYS:
        if not config.get(key):
            raise ValueError(f"{key} is required")
    if not isinstance(config["k"], int) or config["k"] <= 0:
        raise ValueError("k must be a positive integer")
    unknown = [mode for mode in config["query_types"] if mode not in retrieval["query_types"]]
    if unknown:
        raise ValueError(f"unknown query_types: {unknown}")
    return config