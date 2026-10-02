import asyncio

import httpx
import pytest
from tenacity import wait_none

from petroleum_transformations.eia_client import fetch_all, fetch_page, run_sync

URL = "https://example.test/data/"


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(fetch_page.retry, "wait", wait_none())


def run(handler, action):
    async def runner():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await action(client)

    return asyncio.run(runner())


def test_fetch_page_builds_params_and_merges_extra():
    captured = []

    def handler(request):
        captured.append(request.url.params)
        return httpx.Response(200, json={"response": {"total": "1", "data": [{"v": 1}]}})

    body = run(
        handler,
        lambda client: fetch_page(
            client, URL, 10, "2026-01-01", "2026-01-31", "KEY", length=50,
            extra_params={"facets[product][]": ["A", "B"]},
        ),
    )

    params = captured[0]
    assert params["offset"] == "10"
    assert params["length"] == "50"
    assert params["api_key"] == "KEY"
    assert params["start"] == "2026-01-01"
    assert params.get_list("facets[product][]") == ["A", "B"]
    assert body == {"total": "1", "data": [{"v": 1}]}


def test_fetch_all_paginates_until_total_and_keeps_page_order():
    offsets = []

    def handler(request):
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        rows = [{"offset": offset} for _ in range(min(5, 12 - offset))]
        return httpx.Response(200, json={"response": {"total": "12", "data": rows}})

    rows = run(handler, lambda client: fetch_all(client, URL, "s", "e", "KEY", page_size=5))

    assert sorted(offsets) == [0, 5, 10]
    assert [row["offset"] for row in rows] == [0] * 5 + [5] * 5 + [10] * 2


def test_fetch_all_empty_result_makes_single_request():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(200, json={"response": {"total": "0", "data": []}})

    rows = run(handler, lambda client: fetch_all(client, URL, "s", "e", "KEY"))

    assert rows == []
    assert len(calls) == 1


def test_fetch_page_retries_server_errors_then_succeeds():
    attempts = []

    def handler(request):
        attempts.append(1)
        if len(attempts) < 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"response": {"total": "0", "data": []}})

    body = run(handler, lambda client: fetch_page(client, URL, 0, "s", "e", "KEY"))

    assert len(attempts) == 3
    assert body["total"] == "0"


def test_fetch_page_gives_up_after_three_attempts():
    attempts = []

    def handler(request):
        attempts.append(1)
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        run(handler, lambda client: fetch_page(client, URL, 0, "s", "e", "KEY"))

    assert len(attempts) == 3


def test_fetch_page_does_not_retry_client_errors():
    attempts = []

    def handler(request):
        attempts.append(1)
        return httpx.Response(401)

    with pytest.raises(httpx.HTTPStatusError):
        run(handler, lambda client: fetch_page(client, URL, 0, "s", "e", "KEY"))

    assert len(attempts) == 1


def test_run_sync_returns_the_coroutine_result():
    async def value():
        return 42

    assert run_sync(value()) == 42


def test_run_sync_works_inside_a_running_event_loop():
    async def value():
        return 42

    async def outer():
        return run_sync(value())

    assert asyncio.run(outer()) == 42