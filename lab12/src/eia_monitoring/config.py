import json
from typing import Any

SUPPORTED_JUDGES = ("Safety", "RelevanceToQuery", "RetrievalGroundedness", "RetrievalRelevance")
SCORER_KEYS = ("name", "judge", "sample_rate", "filter_string")


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    scorers = config.get("scorers")
    if not scorers:
        raise ValueError("scorers is required")
    names = []
    for scorer in scorers:
        for key in SCORER_KEYS:
            if key not in scorer or scorer[key] in (None, ""):
                raise ValueError(f"{key} is required for every scorer")
        if scorer["judge"] not in SUPPORTED_JUDGES:
            raise ValueError(f"judge must be one of {SUPPORTED_JUDGES}")
        if not 0 < scorer["sample_rate"] <= 1:
            raise ValueError("sample_rate must be greater than 0 and at most 1")
        names.append(scorer["name"])
    if len(set(names)) != len(names):
        raise ValueError("scorer names must be unique")
    return config