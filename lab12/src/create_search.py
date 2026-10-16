import argparse
import logging
import sys

from databricks.ai_search.client import AISearchClient

from eia_embed.config import load_config as load_embed_config
from eia_search.client import create_endpoint, create_index
from eia_search.config import full_index_name, load_config
from lab12_tables.definitions import full_name, load_tables

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--embed-config", required=True)
    parser.add_argument("--tables-config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    settings = load_config(args.config)
    dimension = load_embed_config(args.embed_config)["dimension"]
    tables = load_tables(args.tables_config)
    source = full_name(args.catalog, args.schema, tables["chunk_embeddings"])
    index_name = full_index_name(args.catalog, args.schema, settings)

    client = AISearchClient()
    create_endpoint(client, settings)
    logger.info("Endpoint ready: %s", settings["endpoint_name"])
    create_index(client, settings, source, index_name, dimension)
    logger.info("Index ready: %s (source %s)", index_name, source)


if __name__ == "__main__":
    main(sys.argv[1:])