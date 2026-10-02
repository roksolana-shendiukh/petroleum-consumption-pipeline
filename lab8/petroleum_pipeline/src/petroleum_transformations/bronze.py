from datetime import date, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp


def get_date_window(days: int = 30, today: date | None = None) -> tuple[str, str]:
    today = today or date.today()
    return (today - timedelta(days=days)).isoformat(), today.isoformat()


def build_product_price_mapping(rows) -> dict[str, list[dict]]:
    mapping: dict[str, list[dict]] = {}
    for row in rows:
        mapping.setdefault(row["consumption_product_code"], []).append(
            {"route": row["price_route"], "price_code": row["price_product_code"]}
        )
    return mapping


def extract_product_codes(consumption_data: list[dict]) -> set[str]:
    return {row["product"] for row in consumption_data if row.get("product")}


def find_unmapped_codes(codes: set[str], mapping: dict) -> set[str]:
    return codes - mapping.keys()


def group_codes_by_route(codes: set[str], mapping: dict, route_urls: dict[str, str]) -> dict[str, set[str]]:
    by_route: dict[str, set[str]] = {}
    for code in codes:
        for m in mapping.get(code, []):
            by_route.setdefault(route_urls[m["route"]], set()).add(m["price_code"])
    return by_route


def prepare_bronze(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("load_date", current_date())
        .dropDuplicates(["period", "series"])
    )
