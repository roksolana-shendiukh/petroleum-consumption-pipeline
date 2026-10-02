import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger("eia_ingestion")

REQUEST_TIMEOUT = 30


def is_retryable(error):
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        return status == 429 or status >= 500
    return isinstance(error, httpx.TransportError)


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
        "offset": offset,
        "length": length,
        "api_key": api_key,
    }
    if extra_params:
        params.update(extra_params)
    response = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()["response"]


async def fetch_all(client, url, start, end, api_key, extra_params=None, page_size=5000, max_concurrency=4):
    first = await fetch_page(client, url, 0, start, end, api_key, page_size, extra_params)
    total = int(first["total"])
    logger.info(f"Total rows available: {total}")
    rows = list(first["data"])

    semaphore = asyncio.Semaphore(max_concurrency)

    async def fetch_offset(offset):
        async with semaphore:
            return await fetch_page(client, url, offset, start, end, api_key, page_size, extra_params)

    pages = await asyncio.gather(*(fetch_offset(offset) for offset in range(page_size, total, page_size)))
    for body in pages:
        rows.extend(body["data"])

    logger.info(f"Fetched {len(rows)}/{total} rows")
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