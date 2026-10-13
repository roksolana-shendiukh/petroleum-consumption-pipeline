import json
from typing import Any


def content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        return "".join(parts)
    return str(content)


def _score(value: Any, name: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f"{name} must be an integer from {low} to {high}")
    return value


def parse_verdict(text: str, low: int, high: int, with_groundedness: bool) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("no JSON object in judge response")
    data = json.loads(text[start : end + 1])
    if not isinstance(data.get("refused"), bool):
        raise ValueError("refused must be true or false")
    groundedness = _score(data.get("groundedness"), "groundedness", low, high) if with_groundedness else None
    return {
        "correctness": _score(data.get("correctness"), "correctness", low, high),
        "relevance": _score(data.get("relevance"), "relevance", low, high),
        "groundedness": groundedness,
        "refused": data["refused"],
        "reasoning": str(data.get("reasoning", "")),
    }