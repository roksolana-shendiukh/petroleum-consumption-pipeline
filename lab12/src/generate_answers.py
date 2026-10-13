import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from databricks.ai_search.client import AISearchClient
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from eia_embed.config import load_config as load_embed_config
from eia_generation.config import load_config
from eia_generation.llm import ask
from eia_generation.prompt import build_messages, render_prompt
from eia_generation.rows import build_row
from eia_generation.storage import read_questions
from eia_generation.tracing import (
    configure,
    flush,
    llm_span,
    question_span,
    retriever_span,
    set_documents,
    set_response_preview,
    set_result,
)
from eia_retrieval.config import load_config as load_retrieval_config
from eia_retrieval.query import embed_texts, get_index, search
from eia_retrieval.storage import save_results
from eia_search.config import full_index_name, load_config as load_search_config
from lab12_tables.definitions import full_name, load_tables

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--retrieval-config", required=True)
    parser.add_argument("--search-config", required=True)
    parser.add_argument("--embed-config", required=True)
    parser.add_argument("--tables-config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--warehouse-id", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    config = load_config(args.config)
    retrieval = load_retrieval_config(args.retrieval_config)
    search_settings = load_search_config(args.search_config)
    embed = load_embed_config(args.embed_config)
    tables = load_tables(args.tables_config)
    configure(args.experiment_id, args.warehouse_id)
    spark = SparkSession.builder.getOrCreate()

    questions = read_questions(
        spark, full_name(args.catalog, args.schema, tables["eval_questions"]), config["splits"]
    )
    if not questions:
        raise RuntimeError("No evaluation questions found for the configured splits")

    vectors = []
    index = None
    if "retrieval" in config["modes"]:
        vectors = embed_texts(spark, embed["endpoint"], [question["question"] for question in questions])
        index = get_index(
            AISearchClient(),
            search_settings,
            full_index_name(args.catalog, args.schema, search_settings),
        )

    client = WorkspaceClient()
    run_id = uuid.uuid4().hex
    rows = []
    for position, question in enumerate(questions):
        for mode in config["modes"]:
            with question_span(run_id, question, mode) as span:
                used = []
                if mode == "retrieval":
                    with retriever_span(question["question"], config["query_type"], config["top_k"]) as retriever:
                        used = search(
                            index,
                            retrieval,
                            vectors[position],
                            question["question"],
                            query_type=config["query_type"],
                            top_k=config["top_k"],
                        )
                        set_documents(retriever, used)
                system, user = build_messages(config, mode, question["question"], used)
                with llm_span(config["model"], system, user) as generation:
                    answer, error, latency_ms = ask(
                        client,
                        config["model"],
                        system,
                        user,
                        config["temperature"],
                        config["max_tokens"],
                        config["max_retries"],
                        config["retry_wait_s"],
                    )
                    set_result(generation, answer, error)
                set_result(span, answer, error)
                set_response_preview(answer, error)
            rows.append(
                build_row(
                    run_id,
                    datetime.now(timezone.utc),
                    question,
                    mode,
                    config["model"],
                    config["query_type"] if mode == "retrieval" else None,
                    config["top_k"] if mode == "retrieval" else None,
                    [chunk["chunk_id"] for chunk in used],
                    render_prompt(system, user),
                    answer,
                    error,
                    latency_ms,
                )
            )
        logger.info("Answered %s (%d of %d)", question["question_id"], position + 1, len(questions))

    flush()
    save_results(
        spark,
        rows,
        full_name(args.catalog, args.schema, tables["generation_results"]),
        tables["generation_results"],
    )
    failed = sum(1 for row in rows if row["error"] is not None)
    logger.info("Run %s: saved %d rows, %d failed", run_id, len(rows), failed)


if __name__ == "__main__":
    main(sys.argv[1:])