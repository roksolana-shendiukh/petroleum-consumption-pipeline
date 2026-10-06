from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied, Unauthenticated
from databricks.sdk.service.compute import ClusterDetails, DataSecurityMode
from databricks.sdk.service.jobs import (
    NotebookTask,
    Run,
    RunLifeCycleState,
    RunResultState,
    SubmitTask,
)

logger = logging.getLogger(__name__)

TERMINAL_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}


PERMANENT_ERRORS = (NotFound, PermissionDenied, Unauthenticated)
TRANSIENT_ERRORS = (DatabricksError, OSError)

DEFAULT_RUN_TIMEOUT_SECONDS = 3600
LOCAL_TIMEOUT_MARGIN_SECONDS = 300
MAX_IDEMPOTENCY_TOKEN_LENGTH = 64


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


def get_client() -> WorkspaceClient:
    return WorkspaceClient()


def _check_idempotency_token(token: str | None) -> None:
    if token is not None and len(token) > MAX_IDEMPOTENCY_TOKEN_LENGTH:
        raise ValueError(
            f"Idempotency token is longer than {MAX_IDEMPOTENCY_TOKEN_LENGTH} characters"
        )



def _request_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int,
    node_type_id: str | None,
    custom_tags: dict[str, str] | None,
    policy_id: str | None,
) -> str:
    node_type = node_type_id or w.clusters.select_node_type(local_disk=True, min_memory_gb=16)
    waiter = w.clusters.create(
        cluster_name=name,
        spark_version=w.clusters.select_spark_version(long_term_support=True),
        node_type_id=node_type,
        num_workers=0,
        autotermination_minutes=autotermination_minutes,
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=w.current_user.me().user_name,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        custom_tags={"ResourceClass": "SingleNode", **(custom_tags or {})},
        policy_id=policy_id,
    )
    logger.info("Cluster '%s' requested, id=%s", name, waiter.cluster_id)
    return waiter.cluster_id


def _wait_until_running(w: WorkspaceClient, cluster_id: str, timeout_minutes: int) -> None:
    w.clusters.wait_get_cluster_running(cluster_id, timeout=timedelta(minutes=timeout_minutes))
    logger.info("Cluster %s is running", cluster_id)


def delete_cluster(w: WorkspaceClient, cluster_id: str) -> None:
    try:
        w.clusters.permanent_delete(cluster_id)
    except NotFound:
        logger.info("Cluster %s is already deleted", cluster_id)
        return
    logger.info("Cluster %s deleted", cluster_id)


def _delete_cluster_quietly(w: WorkspaceClient, cluster_id: str) -> None:
    try:
        delete_cluster(w, cluster_id)
    except Exception:
        logger.exception(
            "Could not delete cluster %s; it will stop by autotermination", cluster_id
        )


def find_clusters_by_tags(w: WorkspaceClient, tags: dict[str, str]) -> list[ClusterDetails]:
    return [
        cluster
        for cluster in w.clusters.list()
        if tags.items() <= (cluster.custom_tags or {}).items()
    ]


def create_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int = 15,
    timeout_minutes: int = 30,
    node_type_id: str | None = None,
    custom_tags: dict[str, str] | None = None,
    policy_id: str | None = None,
) -> str:
    cluster_id = _request_cluster(
        w, name, autotermination_minutes, node_type_id, custom_tags, policy_id
    )
    try:
        _wait_until_running(w, cluster_id, timeout_minutes)
    except BaseException:
        _delete_cluster_quietly(w, cluster_id)
        raise
    return cluster_id


@contextmanager
def temporary_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int = 15,
    timeout_minutes: int = 30,
    node_type_id: str | None = None,
    custom_tags: dict[str, str] | None = None,
    policy_id: str | None = None,
) -> Iterator[str]:
    cluster_id = _request_cluster(
        w, name, autotermination_minutes, node_type_id, custom_tags, policy_id
    )
    try:
        _wait_until_running(w, cluster_id, timeout_minutes)
        yield cluster_id
    finally:
        _delete_cluster_quietly(w, cluster_id)


def submit_notebook(
    w: WorkspaceClient,
    notebook_path: str,
    cluster_id: str,
    parameters: dict[str, str] | None = None,
    run_name: str = "platform-automation",
    timeout_seconds: int = DEFAULT_RUN_TIMEOUT_SECONDS,
    idempotency_token: str | None = None,
) -> int:
    _check_idempotency_token(idempotency_token)
    waiter = w.jobs.submit(
        run_name=run_name,
        timeout_seconds=timeout_seconds,
        idempotency_token=idempotency_token,
        tasks=[
            SubmitTask(
                task_key="notebook",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters or {},
                ),
            )
        ],
    )
    logger.info("Notebook %s submitted, run id=%s", notebook_path, waiter.run_id)
    return waiter.run_id


def find_job_id(w: WorkspaceClient, job_name: str) -> int:
    jobs = list(w.jobs.list(name=job_name))
    if not jobs:
        raise LookupError(f"Job '{job_name}' not found")
    if len(jobs) > 1:
        raise LookupError(f"Job name '{job_name}' is not unique: {len(jobs)} jobs found")
    return jobs[0].job_id


def start_job(
    w: WorkspaceClient,
    job_id: int,
    job_parameters: dict[str, str] | None = None,
    idempotency_token: str | None = None,
) -> int:
    _check_idempotency_token(idempotency_token)
    waiter = w.jobs.run_now(
        job_id=job_id,
        job_parameters=job_parameters,
        idempotency_token=idempotency_token,
    )
    logger.info("Job %s started, run id=%s", job_id, waiter.run_id)
    return waiter.run_id


def _collect_failure_details(w: WorkspaceClient, run: Run) -> str | None:
    details = []
    for task in run.tasks or []:
        state = task.state
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
    except Exception as error:
        logger.warning("Could not cancel run %s: %s", run_id, error)


def wait_for_run(
    w: WorkspaceClient,
    run_id: int,
    poll_seconds: int = 10,
    timeout_seconds: int = DEFAULT_RUN_TIMEOUT_SECONDS + LOCAL_TIMEOUT_MARGIN_SECONDS,
    max_consecutive_errors: int = 3,
    cancel_on_abort: bool = True,
    on_update: Callable[[RunResult], None] | None = None,
) -> RunResult:
    deadline = time.monotonic() + timeout_seconds
    last_state = None
    last_url = ""
    errors = 0
    finished = False
    try:
        while True:
            try:
                result = get_run_result(w, run_id)
            except PERMANENT_ERRORS:
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
        if cancel_on_abort and not finished and not isinstance(error, PERMANENT_ERRORS):
            _cancel_run_quietly(w, run_id)
        raise