import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger("eia_ingestion")

REQUEST_TIMEOUT = 30


class WindowTooLargeError(ValueError):
    pass


class IncompleteDownloadError(ValueError):
    pass


def is_retryable(error):
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        return status == 429 or status >= 500
    return isinstance(error, httpx.TransportError)


def year_windows(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    windows = []
    current = first
    while current <= last:
        window_end = min(date(current.year, 12, 31), last)
        windows.append((current.isoformat(), window_end.isoformat()))
        current = window_end + timedelta(days=1)
    return windows


@retry(
    retry=retry_if_exception(is_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=8),
    reraise=True,
)
async def fetch_page(client, url, offset, start, end, api_key, length=5000, extra_params=None):
    params = {
        "frequency": "weekly",
        "data[0]": "value",
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "sort[1][column]": "series",
        "sort[1][direction]": "asc",
        "offset": offset,
        "length": length,
        "api_key": api_key,
    }
    if extra_params:
        params.update(extra_params)
    response = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        raise httpx.HTTPStatusError(
            f"EIA request failed with status {response.status_code} (offset {offset})",
            request=error.request,
            response=error.response,
        ) from None
    return response.json()["response"]


async def fetch_window(client, url, start, end, api_key, extra_params, page_size):
    body = await fetch_page(client, url, 0, start, end, api_key, page_size, extra_params)
    total = int(body["total"])
    if total > page_size:
        raise WindowTooLargeError(
            f"{start}..{end}: {total} rows do not fit one page of {page_size}; use shorter windows"
        )
    rows = list(body["data"])
    if len(rows) != total:
        raise IncompleteDownloadError(f"{start}..{end}: got {len(rows)} rows, API reports {total}")
    return rows


async def fetch_all(client, url, start, end, api_key, extra_params=None, page_size=5000, max_concurrency=4):
    probe = await fetch_page(client, url, 0, start, end, api_key, 1, extra_params)
    expected = int(probe["total"])
    semaphore = asyncio.Semaphore(max_concurrency)

    async def one(window):
        async with semaphore:
            return await fetch_window(client, url, window[0], window[1], api_key, extra_params, page_size)

    pages = await asyncio.gather(*(one(window) for window in year_windows(start, end)))
    rows = [row for page in pages for row in page]
    if len(rows) != expected:
        raise IncompleteDownloadError(
            f"{start}..{end}: downloaded {len(rows)} rows, but the API reports {expected} for the whole period"
        )
    logger.info(f"Fetched {len(rows)} rows for {start}..{end}")
    return rows


async def fetch_many(client, jobs, start, end, api_key, **kwargs):
    return await asyncio.gather(
        *(fetch_all(client, url, start, end, api_key, extra_params=extra, **kwargs) for url, extra in jobs)
    )


def run_sync(coroutine):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coroutine).result()