import logging
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger("eia_ingestion")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=8), reraise=True)
def fetch_page(url, offset, start, end, api_key, length=5000, extra_params=None):
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
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        raise httpx.HTTPStatusError(
            f"EIA request failed with status {response.status_code} (offset {offset})",
            request=error.request,
            response=error.response,
        ) from None
    return response.json()["response"]


def fetch_all(url, start, end, api_key, extra_params=None, page_size=5000):
    all_data = []
    offset = 0
    total = None

    while total is None or offset < total:
        response = fetch_page(
            url, offset=offset, start=start, end=end, api_key=api_key,
            length=page_size, extra_params=extra_params,
        )
        body = response.json()["response"]

        if total is None:
            total = int(body["total"])
            logger.info(f"Total rows available: {total}")

        all_data.extend(body["data"])
        offset += page_size
        logger.info(f"Fetched {len(all_data)}/{total} rows so far")

    return all_data
