from __future__ import annotations

import logging
import time
from typing import NamedTuple

import requests

log = logging.getLogger(__name__)


class Page(NamedTuple):
    content: bytes
    final_url: str


def create_session(http: dict) -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = http["user_agent"]
    return session


def fetch(session: requests.Session, url: str, http: dict) -> Page | None:
    attempts = http["max_attempts"]
    for attempt in range(1, attempts + 1):
        delay = http["request_delay_s"] * attempt
        try:
            response = session.get(url, timeout=http["timeout_s"])
        except (requests.exceptions.InvalidURL, requests.exceptions.MissingSchema) as exc:
            log.error("Invalid URL %s: %s", url, exc)
            return None
        except requests.RequestException as exc:
            log.warning("Request error for %s (attempt %s/%s): %s", url, attempt, attempts, exc)
        else:
            if response.status_code == 200 and response.content:
                return Page(response.content, response.url)
            log.warning(
                "HTTP %s, %s bytes for %s (attempt %s/%s)",
                response.status_code,
                len(response.content),
                url,
                attempt,
                attempts,
            )
            if response.status_code != 200 and response.status_code not in http["retry_statuses"]:
                return None
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = max(delay, min(float(retry_after), http["max_retry_after_s"]))
        if attempt < attempts:
            time.sleep(delay)
    return None


def fetch_cached(
    session: requests.Session, url: str, http: dict, cache: dict[str, Page | None]
) -> Page | None:
    if url in cache:
        return cache[url]
    if cache:
        time.sleep(http["request_delay_s"])
    page = fetch(session, url, http)
    cache[url] = page
    return page