from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from urllib.parse import urlparse

import requests

from .http_client import Page, fetch_cached

log = logging.getLogger(__name__)


def download(
    session: requests.Session,
    targets: list[dict],
    config: dict,
    run_ts: datetime,
    cache: dict[str, Page | None],
) -> tuple[list[tuple], list[str]]:
    rows, failed = [], []
    for index, target in enumerate(targets, start=1):
        page = fetch_cached(session, target["url"], config["http"], cache)
        if page is None:
            failed.append(target["url"])
            log.error("Failed: %s", target["url"])
        else:
            if urlparse(page.final_url).netloc != urlparse(target["url"]).netloc:
                log.warning("Redirected to another host: %s -> %s", target["url"], page.final_url)
            html = page.content.decode(config["http"]["encoding"], errors="replace")
            if "\ufffd" in html:
                log.warning("Undecodable bytes were replaced in %s", target["url"])
            rows.append(
                (
                    target["doc_id"],
                    target["source"],
                    target["doc_type"],
                    target["section"],
                    target["categories"],
                    target["url"],
                    page.final_url,
                    html,
                    hashlib.sha256(page.content).hexdigest(),
                    run_ts,
                )
            )
        if index % config["log_every"] == 0:
            log.info("Downloaded %s/%s", index, len(targets))
    return rows, failed