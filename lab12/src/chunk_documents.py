import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from eia_chunk.config import load_config
from eia_chunk.documents import build_chunks
from eia_chunk.storage import read_parts, save_chunks
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
    source = full_name(args.catalog, args.schema, tables["parsed"])
    target = full_name(args.catalog, args.schema, tables["chunks"])
    spark = SparkSession.builder.getOrCreate()
    chunked_at = datetime.now(timezone.utc)

    parts = [part.asDict() for part in read_parts(spark, source)]
    if not parts:
        raise RuntimeError(f"No parsed parts found in {source}")
    rows = build_chunks(parts, settings, chunked_at)

    save_chunks(spark, rows, target, tables["chunks"])
    sizes = [row["char_count"] for row in rows]
    logger.info(
        "Chunked %d parts into %d chunks (chars min %d, max %d), saved to %s",
        len(parts),
        len(rows),
        min(sizes),
        max(sizes),
        target,
    )


if __name__ == "__main__":
    main(sys.argv[1:])