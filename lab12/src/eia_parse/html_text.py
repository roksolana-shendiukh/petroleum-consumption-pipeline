import re
from typing import Any

from bs4 import BeautifulSoup, Comment, Tag

WHITESPACE = re.compile(r"\s+")


def normalize(text: str) -> str:
    return WHITESPACE.sub(" ", text).strip()


def strip_suffixes(title: str, suffixes: list[str]) -> str:
    for suffix in suffixes:
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
    return title


def extract_title(soup: BeautifulSoup, selectors: list[str], suffixes: list[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if node is not None:
            title = strip_suffixes(normalize(node.get_text(" ")), suffixes)
            if title:
                return title
    return ""


def flatten_tables(soup: BeautifulSoup, container: Tag, separator: str, dropped: set[str]) -> None:
    for table in container.find_all("table"):
        replacement = soup.new_tag("div")
        lines = []
        caption = table.find("caption")
        if caption is not None:
            lines.append(normalize(caption.get_text(" ")))
        for row in table.find_all("tr"):
            cells = [normalize(cell.get_text(" ")) for cell in row.find_all(["th", "td"])]
            cells = [cell for cell in cells if cell and cell not in dropped]
            if cells:
                lines.append(separator.join(cells))
        for line in lines:
            if line and line not in dropped:
                paragraph = soup.new_tag("p")
                paragraph.string = line
                replacement.append(paragraph)
        table.replace_with(replacement)


def join_dangling_markers(lines: list[str], prefix: str) -> list[str]:
    dangling = prefix.strip()
    result = []
    carry = ""
    for line in lines:
        if line == dangling:
            carry = prefix
            continue
        result.append(carry + line)
        carry = ""
    return result


def container_lines(soup: BeautifulSoup, container: Tag, settings: dict[str, Any]) -> list[str]:
    dropped = set(settings["drop_lines"])
    for comment in container.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    for tag in container.find_all(settings["remove_tags"]):
        tag.decompose()
    for title_tag in container.find_all(settings["remove_title_tags"]):
        title_tag.decompose()
    flatten_tables(soup, container, settings["table_cell_separator"], dropped)
    for item in container.find_all("li"):
        item.insert(0, settings["list_item_prefix"])
    for heading in container.find_all(settings["split_tag"]):
        heading.insert(0, settings["heading_marker"])
    for inline in container.find_all(settings["inline_tags"]):
        inline.unwrap()
    container.smooth()
    for text in container.find_all(string=True):
        text.replace_with(normalize(str(text)))
    lines = [line for line in container.get_text("\n").split("\n") if line and line not in dropped]
    return join_dangling_markers(lines, settings["list_item_prefix"])


def pop_last_updated(lines: list[str], prefix: str) -> tuple[list[str], str]:
    kept = []
    value = ""
    for line in lines:
        if line.startswith(prefix):
            if not value:
                value = line[len(prefix):].strip()
            continue
        kept.append(line)
    return kept, value


def split_sections(lines: list[str], marker: str, default_heading: str) -> list[dict[str, str]]:
    marker_text = marker.strip()
    sections = []
    heading = default_heading
    body: list[str] = []

    def flush() -> None:
        if body:
            sections.append({"heading": heading, "text": "\n".join(body)})

    for line in lines:
        if line.startswith(marker_text):
            flush()
            heading = line[len(marker_text):].strip() or default_heading
            body = []
        else:
            body.append(line)
    flush()
    return sections


def parse_html(html: str, rules: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(html, settings["html_parser"])
    container = soup.select_one(rules["container"])
    if container is None:
        raise ValueError(f"container not found: {rules['container']}")
    title = extract_title(soup, rules["title_selectors"], settings["title_suffixes"])
    lines = container_lines(soup, container, settings)
    lines, last_updated = pop_last_updated(lines, settings["last_updated_prefix"])
    sections = split_sections(lines, settings["heading_marker"], rules["default_heading"])
    return {"title": title, "last_updated": last_updated, "sections": sections}