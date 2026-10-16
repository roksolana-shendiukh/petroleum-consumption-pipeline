import argparse
import logging
import sys

from databricks.ai_search.client import AISearchClient

from eia_search.client import delete_endpoint, delete_index
from eia_search.config import full_index_name, load_config

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    settings = load_config(args.config)
    index_name = full_index_name(args.catalog, args.schema, settings)

    client = AISearchClient()
    delete_index(client, settings, index_name)
    logger.info("Index deleted: %s", index_name)
    delete_endpoint(client, settings)
    logger.info("Endpoint deleted: %s", settings["endpoint_name"])


if __name__ == "__main__":
    main(sys.argv[1:])