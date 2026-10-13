import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from eia_embed.config import load_config
from eia_embed.frame import build_frame
from eia_embed.storage import count_invalid, count_rows, read_chunks, save_embeddings
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
    source = full_name(args.catalog, args.schema, tables["chunks"])
    target = full_name(args.catalog, args.schema, tables["chunk_embeddings"])
    spark = SparkSession.builder.getOrCreate()

    chunks = read_chunks(spark, source)
    expected = chunks.count()
    if expected == 0:
        raise RuntimeError(f"No chunks found in {source}")

    frame = build_frame(chunks, tables["chunk_embeddings"], settings, datetime.now(timezone.utc))
    save_embeddings(frame, target)

    saved = count_rows(spark, target)
    invalid = count_invalid(spark, target, settings["dimension"])
    if saved != expected or invalid:
        raise RuntimeError(
            f"Embedding check failed for {target}: chunks={expected}, saved={saved}, invalid_vectors={invalid}"
        )
    logger.info(
        "Embedded %d chunks with %s (dimension %d), saved to %s",
        saved,
        settings["endpoint"],
        settings["dimension"],
        target,
    )


if __name__ == "__main__":
    main(sys.argv[1:])