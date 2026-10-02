import json
from petroleum_transformations.eia_client import fetch_all, fetch_many
from petroleum_transformations.reference import PRODUCT_PRICE_MAPPING

CONSUMPTION_URL = "https://api.eia.gov/v2/petroleum/cons/wpsup/data/"
ROUTE_URLS = {
    "gnd": "https://api.eia.gov/v2/petroleum/pri/gnd/data/",
    "spt": "https://api.eia.gov/v2/petroleum/pri/spt/data/",
}


def active_mapping(mapping_rows):
    return {row["consumption_product_code"]: row for row in mapping_rows if row["is_active"]}


def price_requests(consumption_codes, mapping_rows=PRODUCT_PRICE_MAPPING):
    active = active_mapping(mapping_rows)
    codes_by_route = {}
    for code in consumption_codes:
        row = active.get(code)
        if row:
            codes_by_route.setdefault(row["price_route"], set()).add(row["price_product_code"])
    return [
        (ROUTE_URLS[route], {"facets[product][]": sorted(codes)})
        for route, codes in sorted(codes_by_route.items())
    ]


def unmapped_codes(consumption_codes, mapping_rows=PRODUCT_PRICE_MAPPING):
    return sorted(set(consumption_codes) - active_mapping(mapping_rows).keys())


async def collect(client, start, end, api_key, mapping_rows=PRODUCT_PRICE_MAPPING, max_concurrency=4):
    consumption = await fetch_all(client, CONSUMPTION_URL, start, end, api_key, max_concurrency=max_concurrency)
    codes = {row["product"] for row in consumption if row.get("product")}
    pages = await fetch_many(
        client, price_requests(codes, mapping_rows), start, end, api_key, max_concurrency=max_concurrency
    )
    prices = [row for page in pages for row in page]
    return consumption, prices, unmapped_codes(codes, mapping_rows)


def to_jsonl(records):
    return "\n".join(json.dumps(record) for record in records)


def file_name(prefix, start, end):
    return f"{prefix}_{start}_{end}.json"