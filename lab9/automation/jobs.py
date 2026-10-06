from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

logger = logging.getLogger(__name__)

DEFAULT_RUN_TIMEOUT_SECONDS = 3600
MAX_IDEMPOTENCY_TOKEN_LENGTH = 64


def _check_idempotency_token(token: str | None) -> None:
    if token is not None and len(token) > MAX_IDEMPOTENCY_TOKEN_LENGTH:
        raise ValueError(
            f"Idempotency token has {len(token)} characters, "
            f"the maximum is {MAX_IDEMPOTENCY_TOKEN_LENGTH}"
        )


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
        # starting the wrong job in prod is worse than stopping with an error.
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
