import argparse
import logging
import sys

from eia_monitoring.config import load_config
from eia_monitoring.experiment import configure
from eia_monitoring.scorers import register_scorers

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--warehouse-id", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    config = load_config(args.config)
    configure(args.experiment_id, args.warehouse_id)
    started = register_scorers(config)
    logger.info("Monitoring is active for experiment %s with scorers: %s", args.experiment_id, ", ".join(started))


if __name__ == "__main__":
    main(sys.argv[1:])