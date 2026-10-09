import asyncio
import json
import logging

import websockets

from .normalizer import DATA_TYPES

URL = "wss://stream.aisstream.io/v0/stream"

logger = logging.getLogger("producer.source")


class FatalSourceError(RuntimeError):
    pass


class AisSource:
    def __init__(self, api_key, bounding_boxes, metrics, initial_backoff_seconds, max_backoff_seconds):
        self.subscription = json.dumps(
            {"APIKey": api_key, "BoundingBoxes": bounding_boxes, "FilterMessageTypes": sorted(DATA_TYPES)}
        )
        self.metrics = metrics
        self.initial_backoff = initial_backoff_seconds
        self.max_backoff = max_backoff_seconds

    async def messages(self):
        backoff = self.initial_backoff
        while True:
            try:
                async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(self.subscription)
                    logger.info("Subscribed to aisstream")
                    async for raw in ws:
                        message = json.loads(raw)
                        if "error" in message:
                            self.raise_or_log(message["error"])
                            break
                        backoff = self.initial_backoff
                        yield message
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as error:
                logger.warning("aisstream connection lost: %s", error)

            self.metrics.increment("reconnects")
            logger.warning("Reconnecting to aisstream in %s s, events in this gap are lost", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff)

    @staticmethod
    def raise_or_log(error):
        if "key" in str(error).lower():
            raise FatalSourceError(f"aisstream rejected the subscription: {error}")
        logger.error("aisstream error: %s", error)