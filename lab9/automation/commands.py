from __future__ import annotations

import argparse
import logging

from databricks.sdk import WorkspaceClient

from automation import ci, clusters, jobs, reporting, runs

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_RUN_FAILED = 1
# 2 is used by argparse for wrong arguments, so runtime errors use another code
EXIT_NOT_FINISHED = 3
EXIT_TIMEOUT = 4
EXIT_ERROR = 5
EXIT_INTERRUPTED = 130

# The server-side timeout (Databricks sets TIMEDOUT) must fire before the local one,
# so the local timeout is longer by a safety margin
LOCAL_TIMEOUT_MARGIN_SECONDS = 300


def _exit_code(result: runs.RunResult) -> int:
    if result.succeeded:
        return EXIT_OK
    return EXIT_RUN_FAILED if result.is_finished else EXIT_NOT_FINISHED


def _finish(result: runs.RunResult) -> int:
    reporting.report_result(result)
    return _exit_code(result)


def provision_and_run(w: WorkspaceClient, args: argparse.Namespace) -> int:
    cluster_name = f"{args.cluster_name_prefix}-{ci.unique_suffix()}"
    with clusters.temporary_cluster(
        w,
        cluster_name,
        node_type_id=args.node_type_id,
        policy_id=args.policy_id,
        custom_tags=ci.cluster_tags(),
    ) as cluster_id:
        run_id = jobs.submit_notebook(
            w,
            args.notebook_path,
            cluster_id,
            parameters=args.params,
            timeout_seconds=args.timeout_seconds,
            # The cluster id is part of the token: a new cluster is always a new intended run
            idempotency_token=ci.idempotency_token(
                "provision-and-run", f"{args.notebook_path}:{cluster_id}", args.params
            ),
        )
        # The cluster is deleted when this block ends, so the run could not survive anyway
        result = runs.wait_for_run(
            w,
            run_id,
            timeout_seconds=args.timeout_seconds + LOCAL_TIMEOUT_MARGIN_SECONDS,
            poll_seconds=args.poll_seconds,
            cancel_on_abort=True,
        )
    return _finish(result)


def run_job(w: WorkspaceClient, args: argparse.Namespace) -> int:
    job_id = jobs.find_job_id(w, args.job_name)
    run_id = jobs.start_job(
        w,
        job_id,
        job_parameters=args.params,
        idempotency_token=ci.idempotency_token("run-job", args.job_name, args.params),
    )
    result = runs.wait_for_run(
        w,
        run_id,
        timeout_seconds=args.wait_timeout_seconds,
        poll_seconds=args.poll_seconds,
        cancel_on_abort=args.cancel_on_abort,
    )
    return _finish(result)


def status(w: WorkspaceClient, args: argparse.Namespace) -> int:
    return _finish(runs.get_run_result(w, args.run_id))


def cleanup(w: WorkspaceClient, args: argparse.Namespace) -> int:
    """Delete the clusters created by one CI run.

    Run it once, in a separate job after all other jobs of the workflow. The CiRun tag is
    shared by the whole workflow run, so an earlier cleanup would delete clusters that
    other jobs are still using.
    """
    tags = {**ci.MANAGED_BY_TAG, "CiRun": args.ci_run}
    found = clusters.find_clusters_by_tags(w, tags)
    failed = 0
    for cluster in found:
        if args.dry_run:
            logger.info("Would delete cluster %s (%s)", cluster.cluster_id, cluster.cluster_name)
            continue
        try:
            clusters.delete_cluster(w, cluster.cluster_id)
        except Exception:
            failed += 1
            logger.exception("Could not delete cluster %s", cluster.cluster_id)
    mode = " (dry run)" if args.dry_run else ""
    logger.info("Found %d cluster(s) with tags %s%s", len(found), tags, mode)
    return EXIT_ERROR if failed else EXIT_OK