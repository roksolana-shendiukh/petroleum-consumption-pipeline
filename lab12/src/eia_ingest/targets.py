from __future__ import annotations

import logging
from collections import Counter

import requests

from .discovery import discover_energy_explained_pages, discover_faq_categories
from .http_client import Page

log = logging.getLogger(__name__)


def build_targets(
    session: requests.Session, config: dict, cache: dict[str, Page | None]
) -> list[dict]:
    explained = config["energy_explained"]
    faq = config["faq"]
    targets = []

    for section, pages in discover_energy_explained_pages(session, config, cache).items():
        targets.append(
            {
                "doc_id": f"{explained['doc_id_prefix']}-{section}",
                "source": explained["source"],
                "doc_type": explained["doc_type"],
                "section": section,
                "categories": None,
                "url": f"{explained['base_url']}{section}/",
            }
        )
        for page in pages:
            targets.append(
                {
                    "doc_id": f"{explained['doc_id_prefix']}-{section}-{page.rsplit('.', 1)[0]}",
                    "source": explained["source"],
                    "doc_type": explained["doc_type"],
                    "section": section,
                    "categories": None,
                    "url": f"{explained['base_url']}{section}/{page}",
                }
            )

    faq_categories = discover_faq_categories(session, config, cache)
    for faq_id in sorted(int(value) for value in faq["ids"]):
        if faq_id not in faq_categories:
            log.warning("FAQ %s was not found on the index page", faq_id)
        targets.append(
            {
                "doc_id": f"{faq['doc_id_prefix']}-{faq_id}",
                "source": faq["source"],
                "doc_type": faq["doc_type"],
                "section": None,
                "categories": faq_categories.get(faq_id, []),
                "url": faq["url_template"].format(faq_id=faq_id),
            }
        )

    duplicates = [doc_id for doc_id, count in Counter(t["doc_id"] for t in targets).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"Duplicate doc_id values: {duplicates}")

    log.info("Targets: %s pages", len(targets))
    return targets