import httpx

from petroleum_transformations.eia_client import run_sync
from petroleum_transformations.ingestion import collect, file_name, price_requests, to_jsonl, unmapped_codes

MAPPING = [
    {"consumption_product_code": "A", "price_route": "gnd", "price_product_code": "PA", "is_active": True},
    {"consumption_product_code": "B", "price_route": "gnd", "price_product_code": "PB", "is_active": True},
    {"consumption_product_code": "C", "price_route": "spt", "price_product_code": "PC", "is_active": True},
    {"consumption_product_code": "D", "price_route": None, "price_product_code": None, "is_active": False},
]


def test_price_requests_group_codes_by_route_and_skip_inactive_products():
    requests = price_requests({"A", "B", "C", "D", "X"}, MAPPING)

    assert [extra["facets[product][]"] for _, extra in requests] == [["PA", "PB"], ["PC"]]
    assert "/gnd/" in requests[0][0]
    assert "/spt/" in requests[1][0]


def test_unmapped_codes_lists_codes_without_an_active_mapping():
    assert unmapped_codes({"A", "D", "X"}, MAPPING) == ["D", "X"]


def test_collect_fetches_prices_only_for_mapped_products():
    seen = []

    def handler(request):
        path = request.url.path
        seen.append((path, request.url.params.get_list("facets[product][]")))
        if "/cons/" in path:
            rows = [{"product": "A"}, {"product": "C"}, {"product": "X"}]
        else:
            rows = [{"price_for": path}]
        return httpx.Response(200, json={"response": {"total": str(len(rows)), "data": rows}})

    async def action():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await collect(client, "s", "e", "KEY", MAPPING)

    consumption, prices, unmapped = run_sync(action())

    assert len(consumption) == 3
    assert len(prices) == 2
    assert unmapped == ["X"]
    price_calls = {path: facets for path, facets in seen if "/pri/" in path}
    assert price_calls["/v2/petroleum/pri/gnd/data/"] == ["PA"]
    assert price_calls["/v2/petroleum/pri/spt/data/"] == ["PC"]


def test_to_jsonl_writes_one_record_per_line():
    assert to_jsonl([{"a": 1}, {"b": 2}]) == '{"a": 1}\n{"b": 2}'


def test_file_name_carries_the_period():
    assert file_name("petroleum_raw", "2009-01-01", "2026-10-02") == "petroleum_raw_2009-01-01_2026-10-02.json"