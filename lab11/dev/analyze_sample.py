import argparse
import json
import logging
import random
import statistics
from collections import Counter, defaultdict
from pathlib import Path

SAMPLE_DIR = Path(__file__).parent / "sample"
DEFAULT_INPUT = SAMPLE_DIR / "ais_sample.jsonl"
DEFAULT_OUTPUT = SAMPLE_DIR / "ais_sample_small.jsonl"

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}
SERVICE_TYPES = {"SubscriptionConfirmation"}

POSITION_SENTINELS = {
    "TrueHeading == 511": lambda m: m.get("TrueHeading") == 511,
    "RateOfTurn == -128": lambda m: m.get("RateOfTurn") == -128,
    "Cog >= 360": lambda m: m.get("Cog", 0) >= 360,
    "Sog >= 102.3": lambda m: m.get("Sog", 0) >= 102.3,
    "Timestamp >= 60": lambda m: m.get("Timestamp", 0) >= 60,
    "Latitude/Longitude unavailable": lambda m: abs(m.get("Latitude", 0)) > 90 or abs(m.get("Longitude", 0)) > 180,
}

STATIC_SENTINELS = {
    "ImoNumber == 0": lambda m: m.get("ImoNumber") == 0,
    "Eta not set": lambda m: m.get("Eta", {}).get("Month") == 0,
    "MaximumStaticDraught == 0": lambda m: m.get("MaximumStaticDraught") == 0,
    "Destination empty": lambda m: not m.get("Destination", "").strip(),
}

logger = logging.getLogger("analyze_sample")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--per-type", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def load(path):
    with path.open(encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]
    return [json.loads(line) for line in lines], [len(line.encode("utf-8")) for line in lines]


def body(message):
    return message["Message"][message["MessageType"]]


def data_messages(messages):
    return [m for m in messages if m.get("MessageType") not in SERVICE_TYPES]


def share(count, total):
    return 100 * count / total if total else 0


def report_volume(messages, sizes):
    types = Counter(m.get("MessageType") for m in messages)
    logger.info("Total messages: %d", len(messages))
    for message_type, count in types.most_common():
        logger.info("Type %-32s %6d (%.1f%%)", message_type, count, share(count, len(messages)))
    logger.info("Average JSON size: %.0f bytes", statistics.mean(sizes))


def report_duplicates(messages):
    bodies = Counter(json.dumps(m["Message"], sort_keys=True) for m in messages)
    exact = sum(count - 1 for count in bodies.values() if count > 1)

    keys = Counter()
    for m in messages:
        if m["MessageType"] in POSITION_TYPES:
            b = body(m)
            keys[(m["MessageType"], b["UserID"], b["Latitude"], b["Longitude"], b["Timestamp"])] += 1
    by_key = sum(count - 1 for count in keys.values() if count > 1)

    ships = Counter(m["MetaData"]["MMSI"] for m in messages)
    logger.info("Exact duplicate payloads: %d", exact)
    logger.info("Position duplicates by (type, mmsi, lat, lon, second): %d", by_key)
    logger.info("Distinct ships: %d, max messages per ship: %d", len(ships), max(ships.values()))


def report_time(messages):
    times = [m["MetaData"]["time_utc"] for m in messages]
    out_of_order = sum(1 for a, b in zip(times, times[1:]) if b < a)
    digits = Counter(len(t.split()[1].partition(".")[2]) for t in times)
    logger.info("time_utc range: %s .. %s", times[0], times[-1])
    logger.info("Out-of-order time_utc: %d of %d", out_of_order, len(times))
    logger.info("Fraction digits in time_utc: %s", dict(sorted(digits.items())))


def report_sentinels(messages, types, checks, label):
    rows = [body(m) for m in messages if m["MessageType"] in types]
    for name, check in checks.items():
        count = sum(1 for row in rows if check(row))
        logger.info("%s %-32s %6d (%.1f%%)", label, name, count, share(count, len(rows)))


def report_strings(messages):
    padded = Counter()
    for m in messages:
        for field, value in body(m).items():
            if isinstance(value, str) and value != value.strip():
                padded[field] += 1
        name = m["MetaData"].get("ShipName", "")
        if name != name.strip():
            padded["MetaData.ShipName"] += 1
    logger.info("Fields with padding whitespace: %s", dict(padded.most_common()))


def report_validity(messages):
    valid = Counter((m["MessageType"], body(m).get("Valid")) for m in messages)
    logger.info("Valid flag by type: %s", {f"{t}={v}": c for (t, v), c in valid.items()})

    parts = Counter()
    for m in messages:
        if m["MessageType"] == "StaticDataReport":
            b = body(m)
            parts[(b.get("PartNumber"), b["ReportA"].get("Valid"), b["ReportB"].get("Valid"))] += 1
    logger.info("StaticDataReport (PartNumber, ReportA.Valid, ReportB.Valid): %s", dict(parts))


def write_small_sample(messages, output, per_type, seed):
    by_type = defaultdict(list)
    for m in messages:
        by_type[m.get("MessageType")].append(m)

    rng = random.Random(seed)
    selected = []
    for group in by_type.values():
        selected.extend(rng.sample(group, min(per_type, len(group))))
    selected.sort(key=lambda m: (m.get("MetaData") or {}).get("time_utc", ""))

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as out:
        for m in selected:
            out.write(json.dumps(m, ensure_ascii=False) + "\n")
    logger.info("Small sample with %d messages written to %s", len(selected), output)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    messages, sizes = load(args.input)
    data = data_messages(messages)

    report_volume(messages, sizes)
    report_duplicates(data)
    report_time(data)
    report_sentinels(data, POSITION_TYPES, POSITION_SENTINELS, "Position")
    report_sentinels(data, {"ShipStaticData"}, STATIC_SENTINELS, "Static")
    report_strings(data)
    report_validity(data)
    write_small_sample(messages, args.output, args.per_type, args.seed)


if __name__ == "__main__":
    main()