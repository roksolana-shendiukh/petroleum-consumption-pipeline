import argparse
import asyncio
import logging
import sys
import time
from collections import Counter
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAB_DIR))

from prod.producer.config import load_settings
from prod.producer.source import AisSource

logger = logging.getLogger("check_source")


class CountingMetrics:
    def __init__(self):
        self.counters = Counter()

    def increment(self, name, value=1):
        self.counters[name] += value


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--region")
    return parser.parse_args()


async def listen(source, seconds):
    types = Counter()
    started = time.monotonic()
    async for message in source.messages():
        types[message.get("MessageType")] += 1
        if time.monotonic() - started >= seconds:
            break
    return types, time.monotonic() - started


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    settings = load_settings(["--region", args.region] if args.region else [])

    metrics = CountingMetrics()
    source = AisSource(
        settings.aisstream_api_key,
        settings.bounding_boxes,
        metrics,
        settings.reconnect_initial_backoff_seconds,
        settings.reconnect_max_backoff_seconds,
    )
    types, elapsed = asyncio.run(listen(source, args.seconds))

    total = sum(types.values())
    logger.info("Region %s: %d messages in %.1fs (%.1f rec/s)", settings.region, total, elapsed, total / elapsed)
    for message_type, count in types.most_common():
        logger.info("Type %-32s %d", message_type, count)
    logger.info("Reconnects: %d", metrics.counters["reconnects"])


if __name__ == "__main__":
    main()