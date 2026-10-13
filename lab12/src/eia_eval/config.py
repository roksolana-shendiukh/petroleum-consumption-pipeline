import json
from typing import Any

REQUIRED_KEYS = ("splits", "question_types", "questions")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for key in REQUIRED_KEYS:
        if not config.get(key):
            raise ValueError(f"{key} is required")
    validate_questions(config)
    return config


def validate_questions(config: dict[str, Any]) -> None:
    seen: set[str] = set()
    for item in config["questions"]:
        question_id = item.get("question_id")
        if not question_id:
            raise ValueError("question_id is required")
        if question_id in seen:
            raise ValueError(f"{question_id}: duplicate question_id")
        seen.add(question_id)
        if item.get("split") not in config["splits"]:
            raise ValueError(f"{question_id}: unknown split {item.get('split')}")
        if item.get("question_type") not in config["question_types"]:
            raise ValueError(f"{question_id}: unknown question_type {item.get('question_type')}")
        if item.get("answerable") != bool(item.get("gold_chunk_ids")):
            raise ValueError(f"{question_id}: answerable must match presence of gold_chunk_ids")