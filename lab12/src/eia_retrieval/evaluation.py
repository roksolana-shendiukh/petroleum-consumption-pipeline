from collections import defaultdict
from datetime import datetime
from typing import Any

from eia_retrieval.metrics import dcg_at_k, ndcg_at_k, precision_at_k, recall_at_k, reciprocal_rank

METRICS = ("recall", "precision", "reciprocal_rank", "dcg", "ndcg")


def build_row(
    run_id: str,
    evaluated_at: datetime,
    question: dict[str, Any],
    query_type: str,
    k: int,
    retrieved: list[str],
) -> dict[str, Any]:
    gold = question["gold_chunk_ids"]
    recall = recall_at_k(retrieved, gold, k)
    return {
        "run_id": run_id,
        "evaluated_at": evaluated_at,
        "question_id": question["question_id"],
        "split": question["split"],
        "question_type": question["question_type"],
        "query_type": query_type,
        "k": k,
        "retrieved_chunk_ids": retrieved,
        "gold_chunk_ids": gold,
        "hit": recall > 0,
        "recall": recall,
        "precision": precision_at_k(retrieved, gold, k),
        "reciprocal_rank": reciprocal_rank(retrieved, gold, k),
        "dcg": dcg_at_k(retrieved, gold, k),
        "ndcg": ndcg_at_k(retrieved, gold, k),
    }


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["query_type"], row["split"])].append(row)
        groups[(row["query_type"], "all")].append(row)
    summary = []
    for (query_type, split), items in sorted(groups.items()):
        entry = {"query_type": query_type, "split": split, "questions": len(items)}
        entry["hit_rate"] = sum(item["hit"] for item in items) / len(items)
        for metric in METRICS:
            entry[metric] = sum(item[metric] for item in items) / len(items)
        summary.append(entry)
    return summary