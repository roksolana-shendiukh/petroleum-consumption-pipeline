from datetime import datetime
from typing import Any


def build_row(
    run_id: str,
    generated_at: datetime,
    question: dict[str, Any],
    mode: str,
    model: str,
    query_type: str | None,
    top_k: int | None,
    chunk_ids: list[str],
    prompt: str,
    answer: str | None,
    error: str | None,
    latency_ms: int,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "question_id": question["question_id"],
        "split": question["split"],
        "question_type": question["question_type"],
        "mode": mode,
        "model": model,
        "query_type": query_type,
        "top_k": top_k,
        "context_chunk_ids": chunk_ids,
        "prompt": prompt,
        "answer": answer,
        "error": error,
        "latency_ms": latency_ms,
    }