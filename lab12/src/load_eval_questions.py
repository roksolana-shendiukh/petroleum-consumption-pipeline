import argparse
import logging
import sys
from collections import Counter
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from eia_eval.config import load_config
from eia_eval.questions import build_rows, missing_chunk_ids
from eia_eval.storage import read_chunk_ids, save_questions
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
    chunks_table = full_name(args.catalog, args.schema, tables["chunks"])
    target = full_name(args.catalog, args.schema, tables["eval_questions"])
    spark = SparkSession.builder.getOrCreate()

    missing = missing_chunk_ids(settings["questions"], read_chunk_ids(spark, chunks_table))
    if missing:
        raise ValueError(f"gold_chunk_ids not found in {chunks_table}: {missing}")

    rows = build_rows(
        settings["questions"],
        tables["eval_questions"],
        {"loaded_at": datetime.now(timezone.utc)},
    )
    save_questions(spark, rows, target, tables["eval_questions"])
    logger.info(
        "Saved %d questions to %s (splits %s, types %s)",
        len(rows),
        target,
        dict(Counter(row["split"] for row in rows)),
        dict(Counter(row["question_type"] for row in rows)),
    )


if __name__ == "__main__":
    main(sys.argv[1:])