from datetime import datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lab12_tables.definitions import column_names


def generated_columns(settings: dict[str, Any], embedded_at: datetime) -> dict[str, Column]:
    return {
        "embedding": F.expr(f"CAST(ai_query('{settings['endpoint']}', {settings['text_column']}) AS array<float>)"),
        "embedding_model": F.lit(settings["endpoint"]),
        "embedded_at": F.lit(embedded_at),
    }


def build_frame(
    chunks: DataFrame,
    table: dict[str, Any],
    settings: dict[str, Any],
    embedded_at: datetime,
) -> DataFrame:
    generated = generated_columns(settings, embedded_at)
    return chunks.select(*[generated[name] if name in generated else F.col(name) for name in column_names(table)])