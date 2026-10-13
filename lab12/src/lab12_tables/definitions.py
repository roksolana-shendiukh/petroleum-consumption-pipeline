from __future__ import annotations

import json
from typing import Any


def load_tables(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        tables = json.load(handle)
    for key, table in tables.items():
        for field in ("name", "columns"):
            if not table.get(field):
                raise ValueError(f"tables.{key}.{field} is required")
    return tables


def full_name(catalog: str, schema: str, table: dict[str, Any]) -> str:
    return f"{catalog}.{schema}.{table['name']}"


def column_names(table: dict[str, Any]) -> list[str]:
    return [column["name"] for column in table["columns"]]


def column_ddl(column: dict[str, Any]) -> str:
    suffix = "" if column["nullable"] else " NOT NULL"
    return f"{column['name']} {column['type']}{suffix}"


def columns_ddl(table: dict[str, Any]) -> str:
    return ", ".join(column_ddl(column) for column in table["columns"])


def create_table_ddl(name: str, table: dict[str, Any]) -> str:
    columns = ",\n  ".join(column_ddl(column) for column in table["columns"])
    ddl = f"CREATE TABLE IF NOT EXISTS {name} (\n  {columns}\n)"
    properties = ", ".join(f"'{key}' = '{value}'" for key, value in table.get("properties", {}).items())
    if properties:
        ddl += f"\nTBLPROPERTIES ({properties})"
    return ddl