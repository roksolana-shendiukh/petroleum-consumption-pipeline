import hashlib
import logging
from datetime import datetime
from typing import Any

from eia_parse.html_text import parse_html

logger = logging.getLogger(__name__)


def build_rows(raw: dict[str, Any], settings: dict[str, Any], parsed_at: datetime) -> list[dict[str, Any]]:
    rules = settings["doc_types"][raw["doc_type"]]
    parsed = parse_html(raw["raw_html"], rules, settings)
    sections = parsed["sections"]
    full_text = "\n".join(section["text"] for section in sections)
    if len(full_text) < settings["min_document_chars"]:
        raise ValueError(f"{raw['doc_id']}: text too short ({len(full_text)} chars)")
    content_hash = hashlib.sha256(f"{parsed['title']}\n{full_text}".encode("utf-8")).hexdigest()
    logger.debug("%s: %d parts, %d chars", raw["doc_id"], len(sections), len(full_text))
    return [
        {
            "doc_id": raw["doc_id"],
            "part_index": index,
            "part_heading": section["heading"],
            "part_text": section["text"],
            "char_count": len(section["text"]),
            "title": parsed["title"],
            "source": raw["source"],
            "doc_type": raw["doc_type"],
            "section": raw["section"],
            "categories": raw["categories"],
            "source_url": raw["source_url"],
            "last_updated": parsed["last_updated"],
            "content_hash": content_hash,
            "retrieved_at": raw["retrieved_at"],
            "parsed_at": parsed_at,
        }
        for index, section in enumerate(sections)
    ]