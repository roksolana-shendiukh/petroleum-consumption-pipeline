from typing import Any

from lab12_tables.definitions import column_names


def build_rows(
    questions: list[dict[str, Any]],
    table: dict[str, Any],
    generated: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for item in questions:
        row = {}
        for name in column_names(table):
            if name in generated:
                row[name] = generated[name]
            elif name in item:
                row[name] = item[name]
            else:
                raise ValueError(f"{item.get('question_id')}: {name} is missing")
        rows.append(row)
    return rows


def missing_chunk_ids(questions: list[dict[str, Any]], chunk_ids: set[str]) -> list[str]:
    return sorted(
        f"{item['question_id']}:{chunk_id}"
        for item in questions
        for chunk_id in item["gold_chunk_ids"]
        if chunk_id not in chunk_ids
    )