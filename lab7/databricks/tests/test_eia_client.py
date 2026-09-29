import pytest
import requests

from petroleum_transformations.eia_client import fetch_all, fetch_page

URL = "https://api.eia.gov/v2/test/"


class FakeResponse:
    def __init__(self, body=None, error=None):
        self._body = body
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._body


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Skip tenacity backoff waits so retry tests run instantly."""
    monkeypatch.setattr("time.sleep", lambda s: None)


def paginated_api(rows, calls):
    def fake_get(url, params, timeout):
        calls.append(params)
        off, length = params["offset"], params["length"]
        return FakeResponse({"response": {"total": str(len(rows)), "data": rows[off:off + length]}})
    return fake_get


def test_fetch_page_builds_params_and_merges_extra(monkeypatch):
    calls = []
    monkeypatch.setattr(requests, "get", paginated_api([{"a": 1}], calls))

    fetch_page(URL, offset=0, start="2026-01-01", end="2026-02-01", api_key="KEY",
               extra_params={"facets[product][]": ["A", "B"]})

    p = calls[0]
    assert p["api_key"] == "KEY"
    assert p["start"] == "2026-01-01" and p["end"] == "2026-02-01"
    assert p["frequency"] == "weekly"
    assert p["facets[product][]"] == ["A", "B"]


def test_fetch_all_paginates_until_total(monkeypatch):
    rows = [{"i": i} for i in range(7)]
    calls = []
    monkeypatch.setattr(requests, "get", paginated_api(rows, calls))

    result = fetch_all(URL, "2026-01-01", "2026-02-01", api_key="KEY", page_size=3)

    assert result == rows
    assert [c["offset"] for c in calls] == [0, 3, 6]


def test_fetch_all_empty_result_makes_single_request(monkeypatch):
    calls = []
    monkeypatch.setattr(requests, "get", paginated_api([], calls))

    assert fetch_all(URL, "2026-01-01", "2026-02-01", api_key="KEY") == []
    assert len(calls) == 1


def test_fetch_page_retries_then_succeeds(monkeypatch):
    responses = [
        FakeResponse(error=requests.HTTPError("500")),
        FakeResponse({"response": {"total": "0", "data": []}}),
    ]
    calls = []

    def fake_get(url, params, timeout):
        calls.append(1)
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", fake_get)

    fetch_page(URL, 0, "2026-01-01", "2026-02-01", api_key="KEY")

    assert len(calls) == 2


def test_fetch_page_gives_up_after_three_attempts(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(1)
        return FakeResponse(error=requests.HTTPError("500"))

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        fetch_page(URL, 0, "2026-01-01", "2026-02-01", api_key="KEY")

    assert len(calls) == 3