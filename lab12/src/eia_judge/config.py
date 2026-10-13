import json
from typing import Any

REQUIRED_KEYS = (
    "model",
    "generation_run",
    "temperature",
    "max_tokens",
    "max_retries",
    "retry_wait_s",
    "scale_min",
    "scale_max",
    "system_prompt",
    "context_template",
    "user_template_retrieval",
    "user_template_no_retrieval",
)
USER_PLACEHOLDERS = ("{question}", "{answerable}", "{expected_answer}", "{answer}")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if key not in config or config[key] in (None, "", []):
            raise ValueError(f"{key} is required")
    for key in ("max_tokens", "max_retries", "scale_min", "scale_max"):
        if not isinstance(config[key], int) or config[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if config["scale_min"] >= config["scale_max"]:
        raise ValueError("scale_min must be less than scale_max")
    if config["temperature"] < 0:
        raise ValueError("temperature must not be negative")
    for key in ("user_template_retrieval", "user_template_no_retrieval"):
        for placeholder in USER_PLACEHOLDERS:
            if placeholder not in config[key]:
                raise ValueError(f"{key} must contain {placeholder}")
    if "{context}" not in config["user_template_retrieval"]:
        raise ValueError("user_template_retrieval must contain {context}")
    return config