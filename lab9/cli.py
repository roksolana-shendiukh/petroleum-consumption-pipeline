from __future__ import annotations

import argparse
import logging
import signal
import sys

from databricks.sdk import WorkspaceClient

from automation import ci, commands, jobs, reporting

logger = logging.getLogger("cli")

EXIT_CODES_HELP = """exit codes:
  0    success
  1    the run finished unsuccessfully
  2    wrong arguments
  3    status only: the run has not finished yet (not an error)
  4    timed out while waiting for the run
  5    the command failed
  130  interrupted (Ctrl+C or SIGTERM)
"""

DEFAULT_WAIT_TIMEOUT_SECONDS = 7200


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
        # For Databricks 0 means "no timeout", which would break the local deadline
        raise argparse.ArgumentTypeError("must be greater than 0")
    return value


class _ParamAction(argparse.Action):
    """Collect repeated --param KEY=VALUE into a dict and reject duplicate keys"""

    def __call__(self, parser, namespace, values, option_string=None):
        params = dict(getattr(namespace, self.dest, None) or {})
        key, value = values
        if key in params:
            parser.error(f"duplicate parameter '{key}'")
        params[key] = value
        setattr(namespace, self.dest, params)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Databricks platform automation",
        epilog=EXIT_CODES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

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

    provision = subcommands.add_parser(
        "provision-and-run",
        parents=[run_options],
        help="create a temporary cluster, run a notebook on it, report, delete the cluster",
    )
    provision.add_argument("--notebook-path", required=True)
    provision.add_argument(
        "--timeout-seconds",
        type=_positive_int,
        default=jobs.DEFAULT_RUN_TIMEOUT_SECONDS,
        help="timeout of the run on the Databricks side; the script waits "
        f"{commands.LOCAL_TIMEOUT_MARGIN_SECONDS} seconds longer (default: %(default)s)",
    )
    provision.add_argument("--cluster-name-prefix", default="lab9")
    provision.add_argument("--node-type-id", default=None)
    provision.add_argument("--policy-id", default=None)
    provision.set_defaults(handler=commands.provision_and_run)

    run_job = subcommands.add_parser(
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
    run_job.set_defaults(handler=commands.run_job)

    status = subcommands.add_parser(
        "status",
        help="show the status of a run "
        "(exit code 3 means it has not finished yet, not an error)",
    )
    status.add_argument("--run-id", type=int, required=True)
    status.set_defaults(handler=commands.status)

    current_ci_run = ci.run_key()
    cleanup = subcommands.add_parser(
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
    cleanup.set_defaults(handler=commands.cleanup)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one command and return the exit code; safe to call from tests or a notebook"""
    args = build_parser().parse_args(argv)
    try:
        return args.handler(WorkspaceClient(), args)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        reporting.report_failure(args.command, "Interrupted before the command finished")
        return commands.EXIT_INTERRUPTED
    except TimeoutError as error:
        logger.error("%s", error)
        reporting.report_failure(args.command, str(error))
        return commands.EXIT_TIMEOUT
    except Exception as error:
        logger.exception("Command '%s' failed", args.command)
        reporting.report_failure(args.command, f"{type(error).__name__}: {error}")
        return commands.EXIT_ERROR


def _handle_stop_signal(signum, frame) -> None:
    # Ignore further signals, so that the cleanup (cancel the run, delete the cluster)
    # is not interrupted by the second signal that CI sends a few seconds later.
    # This is deliberate: a hanging cleanup can only be stopped by SIGKILL.
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