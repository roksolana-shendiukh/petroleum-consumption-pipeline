import json
from typing import Any

REQUIRED_KEYS = (
    "max_chunk_chars",
    "min_chunk_chars",
    "overlap_chars",
    "chars_per_token",
    "separators",
    "header_template",
    "chunk_id_format",
)


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if key not in config:
            raise ValueError(f"{key} is required")
    if config["min_chunk_chars"] >= config["max_chunk_chars"]:
        raise ValueError("min_chunk_chars must be smaller than max_chunk_chars")
    if not 0 <= config["overlap_chars"] < config["max_chunk_chars"]:
        raise ValueError("overlap_chars must be between 0 and max_chunk_chars")
    return config