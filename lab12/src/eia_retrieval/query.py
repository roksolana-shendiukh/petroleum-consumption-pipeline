from typing import Any

from databricks.ai_search.client import AISearchClient
from pyspark.sql import SparkSession


def embed_texts(spark: SparkSession, endpoint: str, texts: list[str]) -> list[list[float]]:
    frame = spark.createDataFrame([(position, text) for position, text in enumerate(texts)], "position int, text string")
    rows = (
        frame.selectExpr("position", f"CAST(ai_query('{endpoint}', text) AS array<float>) AS vector")
        .orderBy("position")
        .collect()
    )
    return [list(row["vector"]) for row in rows]


def get_index(client: AISearchClient, search_settings: dict[str, Any], index_name: str) -> Any:
    return client.get_index(endpoint_name=search_settings["endpoint_name"], index_name=index_name)


def search(
    index: Any,
    settings: dict[str, Any],
    vector: list[float],
    text: str,
    filters: dict[str, Any] | None = None,
    query_type: str | None = None,
    top_k: int | None = None,
) -> list[dict[str, Any]]:
    mode = query_type or settings["query_type"]
    if mode not in settings["query_types"]:
        raise ValueError(f"query_type must be one of {settings['query_types']}")
    response = index.similarity_search(
        columns=settings["columns"],
        query_vector=vector,
        query_text=text if mode == "HYBRID" else None,
        filters=filters,
        num_results=top_k or settings["top_k"],
        query_type=mode,
    )
    names = settings["columns"] + ["score"]
    return [dict(zip(names, row)) for row in response["result"]["data_array"]]