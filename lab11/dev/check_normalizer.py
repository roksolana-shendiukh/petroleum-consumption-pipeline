import argparse
import json
import logging
import sys
import time
from collections import Counter
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(LAB_DIR))

from prod.producer.normalizer import POSITIONS, STATIC, normalize
from prod.producer.proto import ais_position_pb2, ais_ship_static_pb2

MESSAGES = {POSITIONS: ais_position_pb2.AisPosition, STATIC: ais_ship_static_pb2.AisShipStatic}
DEFAULT_INPUT = LAB_DIR / "dev" / "sample" / "ais_sample.jsonl"

logger = logging.getLogger("check_normalizer")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    counts, ids, sizes = Counter(), Counter(), []
    with args.input.open(encoding="utf-8") as f:
        for line in f:
            for kind, fields in normalize(json.loads(line)):
                values = {key: value for key, value in fields.items() if value is not None}
                values.update(ingest_ts=time.time_ns() // 1000, producer_id="check", producer_seq=1)
                record = MESSAGES[kind](**values)
                json.loads(record.raw_payload)
                sizes.append(len(record.SerializeToString()))
                counts[(kind, fields["message_type"])] += 1
                ids[fields["event_id"]] += 1

    for (kind, message_type), count in sorted(counts.items()):
        logger.info("%-10s %-32s %d", kind, message_type, count)
    logger.info("Duplicate event_ids: %d", sum(count - 1 for count in ids.values() if count > 1))
    logger.info("Average protobuf record: %.0f bytes", sum(sizes) / len(sizes))


if __name__ == "__main__":
    main()