from __future__ import annotations

import logging
import os

from automation.runs import RunResult

logger = logging.getLogger(__name__)

MAX_SUMMARY_CHARS = 4000


def _state_name(state) -> str | None:
    return state.value if state is not None else None


def _log_result(result: RunResult) -> None:
    logger.info(
        "Run %s: %s / %s | %s",
        result.run_id,
        _state_name(result.life_cycle_state),
        _state_name(result.result_state),
        result.url,
    )
    if result.message:
        logger.info("Message: %s", result.message)
    if result.error:
        logger.error("Run %s failed:\n%s", result.run_id, result.error)


def _write_summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")
    except OSError:
        logger.warning("Could not write the step summary", exc_info=True)


def _write_result_summary(result: RunResult) -> None:
    lines = [
        f"### Databricks run {result.run_id}",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| State | {_state_name(result.life_cycle_state)} |",
        f"| Result | {_state_name(result.result_state)} |",
        f"| Run URL | {result.url} |",
    ]
    if result.message:
        lines += ["", result.message]
    if result.error:
        # The end of a traceback is the useful part, so keep the tail.
        # ~~~~ instead of ``` because a traceback can contain backticks.
        lines += ["", "~~~~", result.error[-MAX_SUMMARY_CHARS:], "~~~~"]
    _write_summary(lines)


def report_result(result: RunResult) -> None:
    _log_result(result)
    _write_result_summary(result)


def report_failure(command: str, text: str) -> None:
    _write_summary(
        [
            f"### Databricks automation: '{command}' did not complete",
            "",
            "~~~~",
            text[-MAX_SUMMARY_CHARS:],
            "~~~~",
        ]
    )