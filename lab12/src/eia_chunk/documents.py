import math
from datetime import datetime
from itertools import groupby
from typing import Any

from langchain_text_splitters import RecursiveCharacterTextSplitter

from eia_chunk.merge import merge_small_parts


def create_splitter(settings: dict[str, Any]) -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=settings["max_chunk_chars"],
        chunk_overlap=settings["overlap_chars"],
        separators=settings["separators"],
        keep_separator="end",
        length_function=len,
    )


def chunk_document(
    parts: list[dict[str, Any]],
    splitter: RecursiveCharacterTextSplitter,
    settings: dict[str, Any],
    chunked_at: datetime,
) -> list[dict[str, Any]]:
    first = parts[0]
    units = merge_small_parts(
        [{"heading": part["part_heading"], "text": part["part_text"]} for part in parts],
        settings["min_chunk_chars"],
        settings["max_chunk_chars"],
    )
    rows = []
    for unit in units:
        header = settings["header_template"].format(title=first["title"] or "", heading=unit["heading"])
        for body in splitter.split_text(unit["text"]):
            index = len(rows)
            chunk_text = header + body
            rows.append(
                {
                    "chunk_id": settings["chunk_id_format"].format(doc_id=first["doc_id"], index=index),
                    "doc_id": first["doc_id"],
                    "chunk_index": index,
                    "part_heading": unit["heading"],
                    "title": first["title"],
                    "source": first["source"],
                    "doc_type": first["doc_type"],
                    "section": first["section"],
                    "categories": first["categories"],
                    "source_url": first["source_url"],
                    "last_updated": first["last_updated"],
                    "content_hash": first["content_hash"],
                    "chunk_text": chunk_text,
                    "char_count": len(chunk_text),
                    "token_estimate": math.ceil(len(chunk_text) / settings["chars_per_token"]),
                    "retrieved_at": first["retrieved_at"],
                    "chunked_at": chunked_at,
                }
            )
    return rows


def build_chunks(parts: list[dict[str, Any]], settings: dict[str, Any], chunked_at: datetime) -> list[dict[str, Any]]:
    splitter = create_splitter(settings)
    rows: list[dict[str, Any]] = []
    for _, group in groupby(parts, key=lambda part: part["doc_id"]):
        rows.extend(chunk_document(list(group), splitter, settings, chunked_at))
    ids = [row["chunk_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate chunk_id values")
    return rows