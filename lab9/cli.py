from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import uuid

import operations as ops

logger = logging.getLogger("cli")

EXIT_OK = 0
EXIT_RUN_FAILED = 1
EXIT_NOT_FINISHED = 3
EXIT_TIMEOUT = 4
EXIT_ERROR = 5
EXIT_INTERRUPTED = 130

EXIT_CODES_HELP = """exit codes:
  0    success
  1    the run finished unsuccessfully
  2    wrong arguments
  3    status only: the run has not finished yet (not an error)
  4    timed out while waiting for the run
  5    the command failed
  130  interrupted (Ctrl+C or SIGTERM)
"""

MANAGED_BY_TAG = {"ManagedBy": "lab9-cli"}
CLUSTER_TAGS = {"Project": "petroleum-pipeline", **MANAGED_BY_TAG}
DEFAULT_WAIT_TIMEOUT_SECONDS = 7200
MAX_SUMMARY_CHARS = 4000


def _ci_run_key() -> str | None:
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        return None
    return f"{run_id}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"


def _cluster_tags() -> dict[str, str]:
    key = _ci_run_key()
    return {**CLUSTER_TAGS, "CiRun": key} if key else dict(CLUSTER_TAGS)


def _idempotency_token(command: str, target: str, params: dict[str, str] | None) -> str | None:
    key = _ci_run_key()
    if key is None:
        return None
    scope = json.dumps(
        [
            os.environ.get("GITHUB_JOB"),
            os.environ.get("GITHUB_ACTION"),
            command,
            target,
            sorted((params or {}).items()),
        ]
    )
    return f"{key}-{hashlib.sha256(scope.encode()).hexdigest()[:12]}"


def _unique_suffix() -> str:
    key = _ci_run_key()
    if key is None:
        return uuid.uuid4().hex[:8]
    return f"{os.environ.get('GITHUB_JOB', 'job')}-{key}-{uuid.uuid4().hex[:4]}"


def _key_value(text: str) -> tuple[str, str]:
    key, separator, value = text.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got '{text}'")
    return key, value


def _positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got '{text}'") from None
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return value


class _ParamAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        params = dict(getattr(namespace, self.dest, None) or {})
        key, value = values
        if key in params:
            parser.error(f"duplicate parameter '{key}'")
        params[key] = value
        setattr(namespace, self.dest, params)


def _state_name(state) -> str | None:
    return state.value if state is not None else None


def _report(result: ops.RunResult) -> None:
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


def _write_result_summary(result: ops.RunResult) -> None:
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
        lines += ["", "~~~~", result.error[-MAX_SUMMARY_CHARS:], "~~~~"]
    _write_summary(lines)


def _write_failure_summary(command: str, text: str) -> None:
    _write_summary(
        [
            f"### Databricks automation: '{command}' did not complete",
            "",
            "~~~~",
            text[-MAX_SUMMARY_CHARS:],
            "~~~~",
        ]
    )


def _exit_code(result: ops.RunResult) -> int:
    if result.succeeded:
        return EXIT_OK
    return EXIT_RUN_FAILED if result.is_finished else EXIT_NOT_FINISHED


def _finish(result: ops.RunResult) -> int:
    _report(result)
    _write_result_summary(result)
    return _exit_code(result)


def cmd_provision_and_run(w, args: argparse.Namespace) -> int:
    cluster_name = f"{args.cluster_name_prefix}-{_unique_suffix()}"
    with ops.temporary_cluster(
        w,
        cluster_name,
        node_type_id=args.node_type_id,
        policy_id=args.policy_id,
        custom_tags=_cluster_tags(),
    ) as cluster_id:
        run_id = ops.submit_notebook(
            w,
            args.notebook_path,
            cluster_id,
            parameters=args.params,
            timeout_seconds=args.timeout_seconds,
            idempotency_token=_idempotency_token(
                "provision-and-run", f"{args.notebook_path}:{cluster_id}", args.params
            ),
        )
        result = ops.wait_for_run(
            w,
            run_id,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds + ops.LOCAL_TIMEOUT_MARGIN_SECONDS,
            cancel_on_abort=True,
        )
    return _finish(result)


def cmd_run_job(w, args: argparse.Namespace) -> int:
    job_id = ops.find_job_id(w, args.job_name)
    run_id = ops.start_job(
        w,
        job_id,
        job_parameters=args.params,
        idempotency_token=_idempotency_token("run-job", args.job_name, args.params),
    )
    result = ops.wait_for_run(
        w,
        run_id,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.wait_timeout_seconds,
        cancel_on_abort=args.cancel_on_abort,
    )
    return _finish(result)


def cmd_status(w, args: argparse.Namespace) -> int:
    return _finish(ops.get_run_result(w, args.run_id))


def cmd_cleanup(w, args: argparse.Namespace) -> int:
    tags = {**MANAGED_BY_TAG, "CiRun": args.ci_run}
    clusters = ops.find_clusters_by_tags(w, tags)
    failed = 0
    for cluster in clusters:
        if args.dry_run:
            logger.info("Would delete cluster %s (%s)", cluster.cluster_id, cluster.cluster_name)
            continue
        try:
            ops.delete_cluster(w, cluster.cluster_id)
        except Exception:
            failed += 1
            logger.exception("Could not delete cluster %s", cluster.cluster_id)
    mode = " (dry run)" if args.dry_run else ""
    logger.info("Found %d cluster(s) with tags %s%s", len(clusters), tags, mode)
    return EXIT_ERROR if failed else EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Databricks platform automation",
        epilog=EXIT_CODES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run_options = argparse.ArgumentParser(add_help=False)
    run_options.add_argument(
        "--param",
        dest="params",
        action=_ParamAction,
        type=_key_value,
        metavar="KEY=VALUE",
        help="parameter for the notebook or the job; can be repeated",
    )
    run_options.add_argument("--poll-seconds", type=_positive_int, default=10)

    provision = commands.add_parser(
        "provision-and-run",
        parents=[run_options],
        help="create a temporary cluster, run a notebook on it, report, delete the cluster",
    )
    provision.add_argument("--notebook-path", required=True)
    provision.add_argument(
        "--timeout-seconds",
        type=_positive_int,
        default=ops.DEFAULT_RUN_TIMEOUT_SECONDS,
        help="timeout of the run on the Databricks side; the script waits "
        f"{ops.LOCAL_TIMEOUT_MARGIN_SECONDS} seconds longer (default: %(default)s)",
    )
    provision.add_argument("--cluster-name-prefix", default="lab9")
    provision.add_argument("--node-type-id", default=None)
    provision.add_argument("--policy-id", default=None)
    provision.set_defaults(handler=cmd_provision_and_run)

    run_job = commands.add_parser(
        "run-job",
        parents=[run_options],
        help="start an existing job, wait for it and report its status",
    )
    run_job.add_argument("--job-name", required=True)
    run_job.add_argument(
        "--wait-timeout-seconds",
        type=_positive_int,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help="how long this script waits for the run (default: %(default)s); the timeout of "
        "the job itself is set in the job definition, not here",
    )
    run_job.add_argument(
        "--cancel-on-abort",
        action="store_true",
        help="cancel the job run if waiting is aborted (by default the run keeps going)",
    )
    run_job.set_defaults(handler=cmd_run_job)

    status = commands.add_parser(
        "status",
        help="show the status of a run "
        "(exit code 3 means it has not finished yet, not an error)",
    )
    status.add_argument("--run-id", type=int, required=True)
    status.set_defaults(handler=cmd_status)

    current_ci_run = _ci_run_key()
    cleanup = commands.add_parser(
        "cleanup",
        help="delete the clusters created by one CI run; run it once, after all other jobs",
    )
    cleanup.add_argument(
        "--ci-run",
        default=current_ci_run,
        required=current_ci_run is None,
        help="value of the CiRun tag, GITHUB_RUN_ID-GITHUB_RUN_ATTEMPT "
        "(default: the current CI run; required outside of CI)",
    )
    cleanup.add_argument(
        "--dry-run",
        action="store_true",
        help="only list the clusters that would be deleted",
    )
    cleanup.set_defaults(handler=cmd_cleanup)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(ops.get_client(), args)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        _write_failure_summary(args.command, "Interrupted before the command finished")
        return EXIT_INTERRUPTED
    except TimeoutError as error:
        logger.error("%s", error)
        _write_failure_summary(args.command, str(error))
        return EXIT_TIMEOUT
    except Exception as error:
        logger.exception("Command '%s' failed", args.command)
        _write_failure_summary(args.command, f"{type(error).__name__}: {error}")
        return EXIT_ERROR


def _handle_stop_signal(signum, frame) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise KeyboardInterrupt


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    signal.signal(signal.SIGINT, _handle_stop_signal)
    signal.signal(signal.SIGTERM, _handle_stop_signal)
    sys.exit(main())


if __name__ == "__main__":
    run()