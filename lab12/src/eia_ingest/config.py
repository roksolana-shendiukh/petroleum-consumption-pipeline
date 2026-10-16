from __future__ import annotations

import json


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        config = json.load(handle)
    for url in (config["energy_explained"]["base_url"], config["faq"]["index_url"]):
        if not url.endswith("/"):
            raise ValueError(f"URL must end with '/': {url}")
    if not isinstance(config["energy_explained"]["sections"], dict):
        raise ValueError("energy_explained.sections must map a section name to its minimum subpage count")
    return config