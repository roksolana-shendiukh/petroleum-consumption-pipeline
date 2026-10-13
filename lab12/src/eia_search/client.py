from datetime import timedelta
from typing import Any

from databricks.ai_search.client import AISearchClient


def create_endpoint(client: AISearchClient, settings: dict[str, Any]) -> None:
    client.create_endpoint_and_wait(
        name=settings["endpoint_name"],
        endpoint_type=settings["endpoint_type"],
        timeout=timedelta(seconds=settings["timeout_s"]),
    )


def create_index(
    client: AISearchClient,
    settings: dict[str, Any],
    source_table: str,
    index_name: str,
    dimension: int,
) -> Any:
    return client.create_delta_sync_index_and_wait(
        endpoint_name=settings["endpoint_name"],
        index_name=index_name,
        source_table_name=source_table,
        pipeline_type=settings["pipeline_type"],
        primary_key=settings["primary_key"],
        embedding_dimension=dimension,
        embedding_vector_column=settings["embedding_vector_column"],
        columns_to_sync=settings["columns_to_sync"],
        timeout=timedelta(seconds=settings["timeout_s"]),
    )


def delete_index(client: AISearchClient, settings: dict[str, Any], index_name: str) -> None:
    client.delete_index(endpoint_name=settings["endpoint_name"], index_name=index_name)


def delete_endpoint(client: AISearchClient, settings: dict[str, Any]) -> None:
    client.delete_endpoint(name=settings["endpoint_name"])