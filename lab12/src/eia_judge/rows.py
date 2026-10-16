from datetime import datetime
from typing import Any

BEHAVIOR_EXCLUDED_TYPES = ("adversarial",)


def expected_behavior_met(answerable: bool, refused: bool) -> bool:
    return refused != answerable


def build_row(
    run_id: str,
    judged_at: datetime,
    generation: dict[str, Any],
    question: dict[str, Any],
    judge_model: str,
    verdict: dict[str, Any] | None,
    error: str | None,
    latency_ms: int,
) -> dict[str, Any]:
    behavior = None
    if verdict is not None and generation["question_type"] not in BEHAVIOR_EXCLUDED_TYPES:
        behavior = expected_behavior_met(question["answerable"], verdict["refused"])
    return {
        "run_id": run_id,
        "judged_at": judged_at,
        "generation_run_id": generation["run_id"],
        "question_id": generation["question_id"],
        "split": generation["split"],
        "question_type": generation["question_type"],
        "mode": generation["mode"],
        "judge_model": judge_model,
        "correctness": verdict["correctness"] if verdict else None,
        "relevance": verdict["relevance"] if verdict else None,
        "groundedness": verdict["groundedness"] if verdict else None,
        "refused": verdict["refused"] if verdict else None,
        "behavior_correct": behavior,
        "reasoning": verdict["reasoning"] if verdict else None,
        "error": error,
        "latency_ms": latency_ms,
    }