import httpx
import pytest
from tenacity import wait_none

from petroleum_transformations.eia_client import (
    IncompleteDownloadError,
    WindowTooLargeError,
    fetch_all,
    fetch_page,
    run_sync,
    year_windows,
)

URL = "https://example.test/data/"

ROWS = [
    {"series": "s", "period": period}
    for period in ("2023-05-05", "2024-03-01", "2024-12-31", "2025-01-03", "2026-02-06")
]


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(fetch_page.retry, "wait", wait_none())


def run(handler, action):
    async def runner():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await action(client)

    return run_sync(runner())


def api(rows, calls, exclusive_end=False):
    def handler(request):
        params = request.url.params
        calls.append(dict(params))
        start, end = params["start"], params["end"]
        selected = [
            row for row in rows
            if start <= row["period"] and (row["period"] < end if exclusive_end else row["period"] <= end)
        ]
        offset, length = int(params["offset"]), int(params["length"])
        page = selected[offset:offset + length]
        return httpx.Response(200, json={"response": {"total": str(len(selected)), "data": page}})

    return handler


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
    assert params["sort[0][column]"] == "period"
    assert params["sort[1][column]"] == "series"
    assert params["sort[1][direction]"] == "asc"
    assert params.get_list("facets[product][]") == ["A", "B"]
    assert body == {"total": "1", "data": [{"v": 1}]}


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


def test_errors_do_not_leak_the_api_key():
    def handler(request):
        return httpx.Response(401)

    with pytest.raises(httpx.HTTPStatusError) as caught:
        run(handler, lambda client: fetch_page(client, URL, 0, "s", "e", "SECRET-KEY"))

    assert "SECRET-KEY" not in str(caught.value)


def test_run_sync_returns_the_coroutine_result():
    async def value():
        return 42

    assert run_sync(value()) == 42


def test_run_sync_works_when_called_from_inside_a_coroutine():
    async def value():
        return 42

    async def outer():
        return run_sync(value())

    assert run_sync(outer()) == 42


def test_year_windows_keeps_a_short_range_in_one_window():
    assert year_windows("2026-09-01", "2026-10-02") == [("2026-09-01", "2026-10-02")]


def test_year_windows_splits_at_the_end_of_each_year():
    assert year_windows("2024-06-15", "2025-02-01") == [
        ("2024-06-15", "2024-12-31"),
        ("2025-01-01", "2025-02-01"),
    ]


def test_year_windows_does_not_split_a_full_year():
    assert year_windows("2024-01-01", "2024-12-31") == [("2024-01-01", "2024-12-31")]


def test_fetch_all_downloads_every_window_and_returns_all_rows():
    calls = []

    rows = run(api(ROWS, calls), lambda client: fetch_all(client, URL, "2023-01-01", "2026-06-30", "KEY"))

    assert sorted(row["period"] for row in rows) == sorted(row["period"] for row in ROWS)
    assert len(calls) == 1 + 4


def test_fetch_all_never_pages_with_an_offset():
    calls = []

    run(api(ROWS, calls), lambda client: fetch_all(client, URL, "2023-01-01", "2026-06-30", "KEY"))

    assert all(call["offset"] == "0" for call in calls)


def test_fetch_all_empty_result_needs_only_the_probe_and_one_window():
    calls = []

    rows = run(api([], calls), lambda client: fetch_all(client, URL, "2026-01-01", "2026-01-31", "KEY"))

    assert rows == []
    assert len(calls) == 2


def test_fetch_all_refuses_a_window_that_does_not_fit_one_page():
    calls = []

    with pytest.raises(WindowTooLargeError):
        run(api(ROWS, calls), lambda client: fetch_all(client, URL, "2023-01-01", "2026-06-30", "KEY", page_size=1))


def test_fetch_all_detects_rows_lost_at_a_window_boundary():
    calls = []

    with pytest.raises(IncompleteDownloadError, match="API reports"):
        run(
            api(ROWS, calls, exclusive_end=True),
            lambda client: fetch_all(client, URL, "2023-01-01", "2026-06-30", "KEY"),
        )