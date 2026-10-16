import json
from typing import Any

REQUIRED_KEYS = ("endpoint", "dimension", "text_column")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if not config.get(key):
            raise ValueError(f"{key} is required")
    if not isinstance(config["dimension"], int) or config["dimension"] <= 0:
        raise ValueError("dimension must be a positive integer")
    return config