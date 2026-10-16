import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from eia_generation.llm import ask
from eia_judge.config import load_config
from eia_judge.parse import content_text, parse_verdict
from eia_judge.prompt import build_messages
from eia_judge.rows import build_row
from eia_judge.storage import read_chunks, read_generations, read_question_map
from eia_retrieval.storage import save_results
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
    config = load_config(args.config)
    tables = load_tables(args.tables_config)
    spark = SparkSession.builder.getOrCreate()

    generation_run, generations = read_generations(
        spark, full_name(args.catalog, args.schema, tables["generation_results"]), config["generation_run"]
    )
    if not generations:
        raise RuntimeError("No generated answers to judge")
    questions = read_question_map(spark, full_name(args.catalog, args.schema, tables["eval_questions"]))
    chunk_ids = sorted({chunk_id for row in generations for chunk_id in row["context_chunk_ids"]})
    chunks = read_chunks(spark, full_name(args.catalog, args.schema, tables["chunks"]), chunk_ids)
    logger.info("Judging %d answers from generation run %s", len(generations), generation_run)

    client = WorkspaceClient()
    run_id = uuid.uuid4().hex
    rows = []
    for position, generation in enumerate(generations):
        question = questions[generation["question_id"]]
        context = [chunks[chunk_id] for chunk_id in generation["context_chunk_ids"]]
        system, user = build_messages(config, generation["mode"], question, generation["answer"], context)
        content, error, latency_ms = ask(
            client,
            config["model"],
            system,
            user,
            config["temperature"],
            config["max_tokens"],
            config["max_retries"],
            config["retry_wait_s"],
        )
        verdict = None
        if error is None:
            try:
                verdict = parse_verdict(
                    content_text(content),
                    config["scale_min"],
                    config["scale_max"],
                    generation["mode"] == "retrieval",
                )
            except ValueError as exc:
                error = f"unparseable judge response: {exc}"
                logger.warning("%s %s: %s", generation["question_id"], generation["mode"], error)
        rows.append(
            build_row(
                run_id,
                datetime.now(timezone.utc),
                generation,
                question,
                config["model"],
                verdict,
                error,
                latency_ms,
            )
        )
        logger.info(
            "Judged %s %s (%d of %d)", generation["question_id"], generation["mode"], position + 1, len(generations)
        )

    save_results(
        spark,
        rows,
        full_name(args.catalog, args.schema, tables["judge_results"]),
        tables["judge_results"],
    )
    failed = sum(1 for row in rows if row["error"] is not None)
    logger.info("Run %s: saved %d rows, %d failed", run_id, len(rows), failed)


if __name__ == "__main__":
    main(sys.argv[1:])