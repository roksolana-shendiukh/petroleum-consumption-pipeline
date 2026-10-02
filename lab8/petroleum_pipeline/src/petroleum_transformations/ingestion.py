import json
from petroleum_transformations.eia_client import fetch_all, fetch_many
from petroleum_transformations.reference import PRODUCT_PRICE_MAPPING

CONSUMPTION_URL = "https://api.eia.gov/v2/petroleum/cons/wpsup/data/"
ROUTE_URLS = {
    "gnd": "https://api.eia.gov/v2/petroleum/pri/gnd/data/",
    "spt": "https://api.eia.gov/v2/petroleum/pri/spt/data/",
}

CONSUMPTION_KEY = ("series", "duoarea", "period")
PRICES_KEY = ("series", "period")


class DuplicateRecordsError(ValueError):
    pass


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


def ensure_unique(records, name, key):
    seen = set()
    duplicates = 0
    for record in records:
        identity = tuple(record.get(field) for field in key)
        if identity in seen:
            duplicates += 1
        seen.add(identity)
    if duplicates:
        raise DuplicateRecordsError(
            f"{name}: {duplicates} records repeat the key {key}; paging is not deterministic"
        )


async def collect(client, start, end, api_key, mapping_rows=PRODUCT_PRICE_MAPPING, max_concurrency=4):
    consumption = await fetch_all(client, CONSUMPTION_URL, start, end, api_key, max_concurrency=max_concurrency)
    codes = {row["product"] for row in consumption if row.get("product")}
    pages = await fetch_many(
        client, price_requests(codes, mapping_rows), start, end, api_key, max_concurrency=max_concurrency
    )
    prices = [row for page in pages for row in page]

    ensure_unique(consumption, "consumption", CONSUMPTION_KEY)
    ensure_unique(prices, "prices", PRICES_KEY)

    return consumption, prices, unmapped_codes(codes, mapping_rows)


def to_jsonl(records):
    return "\n".join(json.dumps(record) for record in records)


def file_name(prefix, start, end, stamp=None):
    suffix = f"_{stamp}" if stamp else ""
    return f"{prefix}_{start}_{end}{suffix}.json"