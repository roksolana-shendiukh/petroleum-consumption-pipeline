import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path

import websockets
from dotenv import load_dotenv

URL = "wss://stream.aisstream.io/v0/stream"

REGIONS = {
    "marmara": [[[40.0, 26.0], [41.5, 30.0]]],
    "northsea": [[[50.0, -5.0], [62.0, 13.0]]],
    "europe": [[[30.0, -15.0], [72.0, 45.0]]],
    "world": [[[-90.0, -180.0], [90.0, 180.0]]],
}

DEFAULT_OUTPUT = Path(__file__).parent / "sample" / "ais_sample.jsonl"

logger = logging.getLogger("ais_smoke_test")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--region", choices=REGIONS.keys(), default="world")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


async def collect(api_key, region, seconds, output):
    counts = Counter()
    examples = {}

    async with websockets.connect(URL) as ws:
        await ws.send(json.dumps({"APIKey": api_key, "BoundingBoxes": REGIONS[region]}))
        logger.info("Connected, region=%s, duration=%ss", region, seconds)

        started = time.monotonic()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as out:
            while time.monotonic() - started < seconds:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                message = json.loads(raw)
                if "error" in message:
                    raise RuntimeError(f"aisstream error: {message['error']}")

                message_type = message.get("MessageType", "unknown")
                counts[message_type] += 1
                examples.setdefault(message_type, message)
                out.write(json.dumps(message, ensure_ascii=False) + "\n")

    return counts, examples, time.monotonic() - started


def report(counts, examples, elapsed, output):
    total = sum(counts.values())
    logger.info("Received %d messages in %.1fs (%.1f rec/s)", total, elapsed, total / elapsed)

    for message_type, count in counts.most_common():
        logger.info("Type %-32s %d", message_type, count)

    for message_type, message in examples.items():
        logger.info("MetaData for %s: %s", message_type, json.dumps(message.get("MetaData")))

    for message_type in ("PositionReport", "ShipStaticData"):
        if message_type in examples:
            logger.info("Full %s: %s", message_type, json.dumps(examples[message_type], indent=2))

    logger.info("Raw messages saved to %s", output)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()

    api_key = os.environ.get("AISSTREAM_API_KEY")
    if not api_key:
        logger.error("AISSTREAM_API_KEY is not set")
        sys.exit(1)

    args = parse_args()
    counts, examples, elapsed = asyncio.run(collect(api_key, args.region, args.seconds, args.output))
    report(counts, examples, elapsed, args.output)


if __name__ == "__main__":
    main()