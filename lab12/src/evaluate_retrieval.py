import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from databricks.ai_search.client import AISearchClient
from pyspark.sql import SparkSession

from eia_embed.config import load_config as load_embed_config
from eia_retrieval.config import load_config, load_evaluation_config
from eia_retrieval.evaluation import build_row, summarize
from eia_retrieval.query import embed_texts, get_index, search
from eia_retrieval.storage import read_answerable_questions, save_results
from eia_search.config import full_index_name, load_config as load_search_config
from lab12_tables.definitions import full_name, load_tables

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--eval-config", required=True)
    parser.add_argument("--search-config", required=True)
    parser.add_argument("--embed-config", required=True)
    parser.add_argument("--tables-config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    retrieval = load_config(args.config)
    evaluation = load_evaluation_config(args.eval_config, retrieval)
    search_settings = load_search_config(args.search_config)
    embed = load_embed_config(args.embed_config)
    tables = load_tables(args.tables_config)
    spark = SparkSession.builder.getOrCreate()

    questions = read_answerable_questions(spark, full_name(args.catalog, args.schema, tables["eval_questions"]))
    if not questions:
        raise RuntimeError("No answerable evaluation questions found")
    vectors = embed_texts(spark, embed["endpoint"], [question["question"] for question in questions])
    index = get_index(
        AISearchClient(),
        search_settings,
        full_index_name(args.catalog, args.schema, search_settings),
    )

    run_id = uuid.uuid4().hex
    evaluated_at = datetime.now(timezone.utc)
    rows = []
    for query_type in evaluation["query_types"]:
        for question, vector in zip(questions, vectors):
            hits = search(index, retrieval, vector, question["question"], query_type=query_type, top_k=evaluation["k"])
            retrieved = [hit["chunk_id"] for hit in hits]
            rows.append(build_row(run_id, evaluated_at, question, query_type, evaluation["k"], retrieved))

    save_results(spark, rows, full_name(args.catalog, args.schema, tables["retrieval_results"]), tables["retrieval_results"])
    logger.info("Run %s: saved %d rows", run_id, len(rows))
    for entry in summarize(rows):
        logger.info(
            "%s %s n=%d hit=%.3f recall=%.3f precision=%.3f mrr=%.3f dcg=%.3f ndcg=%.3f",
            entry["query_type"],
            entry["split"],
            entry["questions"],
            entry["hit_rate"],
            entry["recall"],
            entry["precision"],
            entry["reciprocal_rank"],
            entry["dcg"],
            entry["ndcg"],
        )


if __name__ == "__main__":
    main(sys.argv[1:])