import os
from contextlib import contextmanager
from typing import Any, Iterator

import mlflow
from mlflow.entities import Document, SpanType


def configure(experiment_id: str, warehouse_id: str) -> None:
    if not experiment_id:
        raise ValueError("experiment_id is required")
    if not warehouse_id:
        raise ValueError("warehouse_id is required")
    os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=experiment_id)


@contextmanager
def question_span(run_id: str, question: dict[str, Any], mode: str) -> Iterator[Any]:
    with mlflow.start_span(name="answer_question", span_type=SpanType.CHAIN) as span:
        span.set_inputs({"question_id": question["question_id"], "question": question["question"], "mode": mode})
        mlflow.update_current_trace(
            tags={
                "run_id": run_id,
                "question_id": question["question_id"],
                "split": question["split"],
                "question_type": question["question_type"],
                "mode": mode,
            },
            request_preview=question["question"],
        )
        yield span


@contextmanager
def retriever_span(query: str, query_type: str, top_k: int) -> Iterator[Any]:
    with mlflow.start_span(name="retrieve", span_type=SpanType.RETRIEVER) as span:
        span.set_inputs({"query": query, "query_type": query_type, "top_k": top_k})
        yield span


@contextmanager
def llm_span(model: str, system: str, user: str) -> Iterator[Any]:
    with mlflow.start_span(name="generate", span_type=SpanType.LLM) as span:
        span.set_inputs(
            {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            }
        )
        span.set_attributes({"model": model})
        yield span


def set_documents(span: Any, chunks: list[dict[str, Any]]) -> None:
    span.set_outputs(
        [
            Document(
                page_content=chunk["chunk_text"],
                metadata={"doc_uri": chunk["source_url"], "chunk_id": chunk["chunk_id"], "score": chunk["score"]},
                id=chunk["chunk_id"],
            )
            for chunk in chunks
        ]
    )


def set_result(span: Any, answer: str | None, error: str | None) -> None:
    span.set_outputs(answer if error is None else {"error": error})


def set_response_preview(answer: str | None, error: str | None) -> None:
    mlflow.update_current_trace(response_preview=answer if error is None else f"error: {error}")


def flush() -> None:
    mlflow.flush_trace_async_logging()