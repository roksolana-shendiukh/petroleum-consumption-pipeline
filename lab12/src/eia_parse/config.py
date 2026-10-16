import json
from typing import Any


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    if not config.get("doc_types"):
        raise ValueError("config must define doc_types")
    for doc_type, rules in config["doc_types"].items():
        for key in ("container", "title_selectors", "default_heading"):
            if not rules.get(key):
                raise ValueError(f"doc_types.{doc_type}.{key} is required")
    return config