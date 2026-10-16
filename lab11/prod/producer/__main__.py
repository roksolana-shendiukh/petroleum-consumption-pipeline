import asyncio
import logging
import time

from .config import load_settings
from .metrics import Metrics
from .normalizer import normalize
from .sink import ZerobusSink
from .source import AisSource

logger = logging.getLogger("producer")


async def run(settings):
    metrics = Metrics()
    sink = ZerobusSink(settings, metrics)
    source = AisSource(
        settings.aisstream_api_key,
        settings.bounding_boxes,
        metrics,
        settings.reconnect_initial_backoff_seconds,
        settings.reconnect_max_backoff_seconds,
    )
    deadline = time.monotonic() + settings.duration_seconds if settings.duration_seconds else None

    logger.info(
        "Starting producer_id=%s region=%s duration=%s",
        settings.producer_id,
        settings.region,
        f"{settings.duration_seconds}s" if deadline else "until Ctrl+C",
    )
    await sink.open()
    reporter = asyncio.create_task(metrics.report_every(settings.metrics_interval_seconds))
    try:
        async for message in source.messages():
            metrics.increment("received")
            try:
                events = normalize(message)
            except (KeyError, TypeError, ValueError) as error:
                metrics.increment("malformed")
                logger.warning("Malformed %s message: %r", message.get("MessageType"), error)
                continue
            if not events:
                metrics.increment("skipped")
            for kind, fields in events:
                await sink.send(kind, fields)
            if deadline and time.monotonic() >= deadline:
                logger.info("Duration reached, stopping")
                break
    finally:
        reporter.cancel()
        await sink.close()
        metrics.log("final")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    main()