from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied, Unauthenticated
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState

logger = logging.getLogger(__name__)

TERMINAL_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}

PERMANENT_ERRORS = (NotFound, PermissionDenied, Unauthenticated)
TRANSIENT_ERRORS = (DatabricksError, OSError)


@dataclass(frozen=True)
class RunResult:
    run_id: int
    life_cycle_state: RunLifeCycleState | None
    result_state: RunResultState | None
    message: str
    url: str
    error: str | None = None

    @property
    def is_finished(self) -> bool:
        return self.life_cycle_state in TERMINAL_STATES

    @property
    def succeeded(self) -> bool:
        return self.is_finished and self.result_state == RunResultState.SUCCESS


def _collect_failure_details(w: WorkspaceClient, run: Run) -> str | None:
    details = []
    for task in run.tasks or []:
        state = task.state
        # Skipped and internal-error tasks have no result_state, so keep everything but SUCCESS
        if state is not None and state.result_state == RunResultState.SUCCESS:
            continue

        text = ""
        try:
            output = w.jobs.get_run_output(task.run_id)
            text = "\n".join(part for part in (output.error, output.error_trace) if part)
        except Exception:
            logger.warning("Could not read the output of task %s", task.task_key, exc_info=True)

        if not text and state is not None:
            life_cycle = state.life_cycle_state.value if state.life_cycle_state else ""
            text = state.state_message or life_cycle
        details.append(f"[{task.task_key}] {text or 'no details'}")
    return "\n\n".join(details) or None


def get_run_result(w: WorkspaceClient, run_id: int) -> RunResult:
    run = w.jobs.get_run(run_id=run_id)
    state = run.state
    life_cycle_state = state.life_cycle_state if state else None
    result_state = state.result_state if state else None

    # Reading task output costs extra API calls, so only do it for finished, failed runs
    error = None
    if life_cycle_state in TERMINAL_STATES and result_state != RunResultState.SUCCESS:
        error = _collect_failure_details(w, run)

    return RunResult(
        run_id=run_id,
        life_cycle_state=life_cycle_state,
        result_state=result_state,
        message=(state.state_message if state else "") or "",
        url=run.run_page_url or "",
        error=error,
    )


def _cancel_run_quietly(w: WorkspaceClient, run_id: int) -> None:
    try:
        w.jobs.cancel_run(run_id=run_id)
        logger.warning("Run %s cancelled", run_id)
    # broad on purpose: cancelling is best effort and must not replace the original error
    except Exception as error:
        logger.warning("Could not cancel run %s: %s", run_id, error)


def wait_for_run(
    w: WorkspaceClient,
    run_id: int,
    timeout_seconds: int,
    poll_seconds: int = 10,
    max_consecutive_errors: int = 3,
    cancel_on_abort: bool = True,
    on_update: Callable[[RunResult], None] | None = None,
) -> RunResult:
    """Poll a run until it finishes and return its final status"""
    deadline = time.monotonic() + timeout_seconds
    last_state = None
    last_url = ""
    errors = 0
    finished = False # True once a finished run was seen: such a run needs no cancelling
    try:
        while True:
            try:
                result = get_run_result(w, run_id)
            except PERMANENT_ERRORS:
                # first on purpose: these are also DatabricksError, so TRANSIENT_ERRORS would catch them
                raise
            except TRANSIENT_ERRORS:
                errors += 1
                if errors >= max_consecutive_errors:
                    raise
                logger.warning(
                    "Could not read run %s (error %d of %d), retrying",
                    run_id,
                    errors,
                    max_consecutive_errors,
                    exc_info=True,
                )
            else:
                errors = 0
                finished = result.is_finished
                last_url = result.url or last_url
                if result.life_cycle_state != last_state:
                    logger.info("Run %s: %s (%s)", run_id, result.life_cycle_state, result.url)
                    last_state = result.life_cycle_state
                if on_update:
                    on_update(result)
                if finished:
                    return result

            if time.monotonic() >= deadline:
                where = f" ({last_url})" if last_url else ""
                raise TimeoutError(
                    f"Run {run_id} did not finish in {timeout_seconds} seconds{where}"
                )
            time.sleep(poll_seconds)
    except BaseException as error:
        # Timeout, Ctrl+C, SIGTERM or repeated read errors: do not leave the run unwatched
        # Nothing to cancel if it already finished or if cannot reach it at all
        if cancel_on_abort and not finished and not isinstance(error, PERMANENT_ERRORS):
            _cancel_run_quietly(w, run_id)
        raise
