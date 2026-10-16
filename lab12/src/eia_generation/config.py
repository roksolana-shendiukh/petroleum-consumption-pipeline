import json
from typing import Any

REQUIRED_KEYS = (
    "model",
    "modes",
    "splits",
    "query_type",
    "top_k",
    "temperature",
    "max_tokens",
    "max_retries",
    "retry_wait_s",
    "system_prompt_no_retrieval",
    "system_prompt_retrieval",
    "context_template",
    "user_template_retrieval",
    "user_template_no_retrieval",
    "refusal_phrase",
)
MODES = ("no_retrieval", "retrieval")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if key not in config or config[key] in (None, "", []):
            raise ValueError(f"{key} is required")
    unknown = [mode for mode in config["modes"] if mode not in MODES]
    if unknown:
        raise ValueError(f"unknown modes: {unknown}")
    for key in ("top_k", "max_tokens", "max_retries"):
        if not isinstance(config[key], int) or config[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if config["temperature"] < 0:
        raise ValueError("temperature must not be negative")
    if "{context}" not in config["user_template_retrieval"]:
        raise ValueError("user_template_retrieval must contain {context}")
    for key in ("user_template_retrieval", "user_template_no_retrieval"):
        if "{question}" not in config[key]:
            raise ValueError(f"{key} must contain {{question}}")
    return config