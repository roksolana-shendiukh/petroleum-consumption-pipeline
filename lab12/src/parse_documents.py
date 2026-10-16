import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from eia_parse.config import load_config
from eia_parse.documents import build_rows
from eia_parse.storage import read_raw, save_parsed
from lab12_tables.definitions import full_name, load_tables

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--tables-config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    settings = load_config(args.config)
    tables = load_tables(args.tables_config)
    source = full_name(args.catalog, args.schema, tables["raw"])
    target = full_name(args.catalog, args.schema, tables["parsed"])
    spark = SparkSession.builder.getOrCreate()
    parsed_at = datetime.now(timezone.utc)

    rows = []
    failed = []
    documents = read_raw(spark, source)
    for raw in documents:
        try:
            rows.extend(build_rows(raw.asDict(), settings, parsed_at))
        except ValueError as error:
            logger.error("Parsing failed: %s", error)
            failed.append(raw["doc_id"])
    if failed:
        raise RuntimeError(f"Parsing failed for {len(failed)} documents: {failed}")

    save_parsed(spark, rows, target, tables["parsed"])
    logger.info("Parsed %d documents into %d parts, saved to %s", len(documents), len(rows), target)


if __name__ == "__main__":
    main(sys.argv[1:])