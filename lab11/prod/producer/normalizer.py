import hashlib
from datetime import datetime

POSITIONS = "positions"
STATIC = "static"

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}
STATIC_TYPES = {"ShipStaticData", "StaticDataReport", "ExtendedClassBPositionReport"}
DATA_TYPES = POSITION_TYPES | STATIC_TYPES


def parse_time_utc(value):
    date_part, time_part, offset = value.split()[:3]
    clock, _, fraction = time_part.partition(".")
    parsed = datetime.strptime(f"{date_part} {clock} {offset}", "%Y-%m-%d %H:%M:%S %z")
    micros = int(fraction.ljust(9, "0")[:6]) if fraction else 0
    return int(parsed.timestamp()) * 1_000_000 + micros


def event_id(*parts):
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def clean_text(value):
    if not isinstance(value, str):
        return None
    cleaned = value.replace("@", "").strip()
    return cleaned or None


def in_range(value, low, high):
    if value is None:
        return None
    return value if low <= value <= high else None


def heading(value):
    return None if value == 511 else in_range(value, 0, 359)


def course(value):
    return in_range(value, 0, 359.9)


def speed(value):
    return in_range(value, 0, 102.2)


def rate_of_turn(value):
    return None if value == -128 else value


def utc_second(value):
    return in_range(value, 0, 59)


def positive(value):
    return value if value else None


def dimensions(dimension):
    if not dimension or not any(dimension.get(key) for key in ("A", "B", "C", "D")):
        return {}
    return {
        "dim_to_bow": dimension.get("A"),
        "dim_to_stern": dimension.get("B"),
        "dim_to_port": dimension.get("C"),
        "dim_to_starboard": dimension.get("D"),
    }


def eta(value):
    if not value or not 1 <= value.get("Month", 0) <= 12 or not 1 <= value.get("Day", 0) <= 31:
        return {}
    return {
        "eta_month": value["Month"],
        "eta_day": value["Day"],
        "eta_hour": in_range(value.get("Hour"), 0, 23),
        "eta_minute": in_range(value.get("Minute"), 0, 59),
    }


def common(message_type, mmsi, event_ts, key):
    return {"event_id": key, "message_type": message_type, "mmsi": mmsi, "event_ts": event_ts}


def position(message_type, body, meta, mmsi, event_ts):
    latitude, longitude = body.get("Latitude"), body.get("Longitude")
    fields = common(message_type, mmsi, event_ts, event_id(message_type, mmsi, meta["time_utc"], latitude, longitude))
    fields.update(
        ship_name=clean_text(body.get("Name")) or clean_text(meta.get("ShipName")),
        latitude=in_range(latitude, -90, 90),
        longitude=in_range(longitude, -180, 180),
        sog=speed(body.get("Sog")),
        cog=course(body.get("Cog")),
        true_heading=heading(body.get("TrueHeading")),
        nav_status=body.get("NavigationalStatus"),
        rate_of_turn=rate_of_turn(body.get("RateOfTurn")),
        position_accuracy=body.get("PositionAccuracy"),
        raim=body.get("Raim"),
        utc_second=utc_second(body.get("Timestamp")),
        message_id=body.get("MessageID"),
        repeat_indicator=body.get("RepeatIndicator"),
    )
    return fields


def static(message_type, body, meta, mmsi, event_ts):
    if message_type == "StaticDataReport":
        part = "B" if body.get("PartNumber") else "A"
        report = body.get("ReportB" if part == "B" else "ReportA") or {}
        if not report.get("Valid"):
            return None
    else:
        part = "full"
        report = body

    fields = common(message_type, mmsi, event_ts, event_id(message_type, mmsi, meta["time_utc"], part))
    fields.update(
        report_part=part,
        ship_name=clean_text(report.get("Name")),
        call_sign=clean_text(report.get("CallSign")),
        imo_number=positive(report.get("ImoNumber")),
        ship_type=positive(report.get("Type", report.get("ShipType"))),
        draught=positive(report.get("MaximumStaticDraught")),
        destination=clean_text(report.get("Destination")),
    )
    fields.update(dimensions(report.get("Dimension")))
    fields.update(eta(report.get("Eta")))
    return fields


def normalize(message):
    message_type = message.get("MessageType")
    if message_type not in DATA_TYPES:
        return []

    body = message["Message"][message_type]
    if not body.get("Valid", True):
        return []

    meta = message["MetaData"]
    mmsi = int(body["UserID"])
    event_ts = parse_time_utc(meta["time_utc"])

    events = []
    if message_type in POSITION_TYPES:
        events.append((POSITIONS, position(message_type, body, meta, mmsi, event_ts)))
    if message_type in STATIC_TYPES:
        fields = static(message_type, body, meta, mmsi, event_ts)
        if fields:
            events.append((STATIC, fields))
    return events