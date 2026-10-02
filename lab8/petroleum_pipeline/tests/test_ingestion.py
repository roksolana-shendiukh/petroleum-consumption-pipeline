import httpx
import pytest

from petroleum_transformations.eia_client import run_sync
from petroleum_transformations.ingestion import (
    DuplicateRecordsError,
    collect,
    ensure_unique,
    file_name,
    price_requests,
    to_jsonl,
    unmapped_codes,
)

MAPPING = [
    {"consumption_product_code": "A", "price_route": "gnd", "price_product_code": "PA", "is_active": True},
    {"consumption_product_code": "B", "price_route": "gnd", "price_product_code": "PB", "is_active": True},
    {"consumption_product_code": "C", "price_route": "spt", "price_product_code": "PC", "is_active": True},
    {"consumption_product_code": "D", "price_route": None, "price_product_code": None, "is_active": False},
]

START, END = "2026-01-01", "2026-01-31"


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
            rows = [
                {"product": "A", "series": "s1", "duoarea": "NUS", "period": "2026-01-02"},
                {"product": "C", "series": "s2", "duoarea": "NUS", "period": "2026-01-02"},
                {"product": "X", "series": "s3", "duoarea": "NUS", "period": "2026-01-02"},
            ]
        else:
            rows = [{"series": path, "period": "2026-01-02"}]
        return httpx.Response(200, json={"response": {"total": str(len(rows)), "data": rows}})

    async def action():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await collect(client, START, END, "KEY", MAPPING)

    consumption, prices, unmapped = run_sync(action())

    assert len(consumption) == 3
    assert len(prices) == 2
    assert unmapped == ["X"]
    price_calls = {path: facets for path, facets in seen if "/pri/" in path}
    assert price_calls["/v2/petroleum/pri/gnd/data/"] == ["PA"]
    assert price_calls["/v2/petroleum/pri/spt/data/"] == ["PC"]


def test_collect_fails_when_the_api_repeats_a_record():
    def handler(request):
        if "/cons/" in request.url.path:
            rows = [{"product": "A", "series": "s1", "duoarea": "NUS", "period": "2026-01-02"}] * 2
        else:
            rows = []
        return httpx.Response(200, json={"response": {"total": str(len(rows)), "data": rows}})

    async def action():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await collect(client, START, END, "KEY", MAPPING)

    with pytest.raises(DuplicateRecordsError, match="consumption"):
        run_sync(action())


def test_ensure_unique_accepts_distinct_keys():
    ensure_unique([{"series": "A", "period": "1"}, {"series": "A", "period": "2"}], "prices", ("series", "period"))


def test_ensure_unique_rejects_a_repeated_key():
    records = [{"series": "A", "period": "1"}, {"series": "A", "period": "1"}]

    with pytest.raises(DuplicateRecordsError, match="prices"):
        ensure_unique(records, "prices", ("series", "period"))


def test_to_jsonl_writes_one_record_per_line():
    assert to_jsonl([{"a": 1}, {"b": 2}]) == '{"a": 1}\n{"b": 2}'


def test_file_name_carries_the_period():
    assert file_name("petroleum_raw", "2009-01-01", "2026-10-02") == "petroleum_raw_2009-01-01_2026-10-02.json"


def test_file_name_can_carry_a_timestamp():
    assert file_name("p", "2009-01-01", "2026-10-02", "20261002T120000") == "p_2009-01-01_2026-10-02_20261002T120000.json"