from __future__ import annotations

import logging
from collections import defaultdict
from urllib.parse import ParseResult, parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .http_client import Page, fetch_cached

log = logging.getLogger(__name__)


def resolve_internal(href: str, base_url: str) -> ParseResult | None:
    base = urlparse(base_url)
    parsed = urlparse(urljoin(base_url, href))
    if parsed.netloc != base.netloc or not parsed.path.startswith(base.path):
        return None
    return parsed


def section_and_page(href: str, base_url: str, page_suffix: str) -> tuple[str, str] | None:
    parsed = resolve_internal(href, base_url)
    if parsed is None:
        return None
    segments = [segment for segment in parsed.path[len(urlparse(base_url).path):].split("/") if segment]
    if len(segments) != 2 or not segments[1].endswith(page_suffix):
        return None
    return segments[0], segments[1]


def discover_energy_explained_pages(
    session: requests.Session, config: dict, cache: dict[str, Page | None]
) -> dict[str, list[str]]:
    explained = config["energy_explained"]
    landing = f"{explained['base_url']}{explained['landing_section']}/"
    page = fetch_cached(session, landing, config["http"], cache)
    if page is None:
        raise RuntimeError(f"Cannot open Energy Explained landing page: {landing}")
    soup = BeautifulSoup(page.content, "html.parser", from_encoding=config["http"]["encoding"])
    pages = defaultdict(set)
    for link in soup.find_all("a", href=True):
        parsed = section_and_page(link["href"], explained["base_url"], explained["page_suffix"])
        if parsed and parsed[0] in explained["sections"]:
            pages[parsed[0]].add(parsed[1])
    for section, minimum in explained["sections"].items():
        count = len(pages[section])
        log.info("Energy Explained %s: %s subpages", section, count)
        if count < minimum:
            raise RuntimeError(f"Section {section}: found {count} subpages, expected at least {minimum}")
    return {section: sorted(pages[section]) for section in explained["sections"]}


def discover_faq_categories(
    session: requests.Session, config: dict, cache: dict[str, Page | None]
) -> dict[int, list[str]]:
    faq = config["faq"]
    page = fetch_cached(session, faq["index_url"], config["http"], cache)
    if page is None:
        raise RuntimeError(f"Cannot open FAQ index: {faq['index_url']}")
    soup = BeautifulSoup(page.content, "html.parser", from_encoding=config["http"]["encoding"])
    categories = defaultdict(set)
    for link in soup.find_all("a", href=True):
        parsed = resolve_internal(link["href"], faq["index_url"])
        if parsed is None or not parsed.path.endswith(faq["link_page"]):
            continue
        values = parse_qs(parsed.query).get(faq["id_param"])
        if not values or not values[0].isdigit():
            continue
        heading = link.find_previous(faq["category_heading_tags"])
        if heading is not None:
            categories[int(values[0])].add(heading.get_text(" ", strip=True))
    if not categories:
        raise RuntimeError(f"No FAQ links found on the index page: {faq['index_url']}")
    return {faq_id: sorted(names) for faq_id, names in categories.items()}