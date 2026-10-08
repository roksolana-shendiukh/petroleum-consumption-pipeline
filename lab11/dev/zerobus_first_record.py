import logging
import os
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

PROTO_DIR = Path(__file__).resolve().parent.parent / "prod" / "producer" / "proto"
sys.path.insert(0, str(PROTO_DIR))

import ais_position_pb2
import ais_ship_static_pb2

PRODUCER_ID = "dev-first-record"
TEST_MMSI = 999999999

logger = logging.getLogger("zerobus_first_record")


def epoch_micros():
    return time.time_ns() // 1000


def position_record():
    now = epoch_micros()
    return ais_position_pb2.AisPosition(
        event_id=f"test-{uuid.uuid4()}",
        message_type="PositionReport",
        mmsi=TEST_MMSI,
        event_ts=now,
        ingest_ts=now,
        producer_id=PRODUCER_ID,
        producer_seq=1,
        ship_name="TEST VESSEL",
        latitude=46.4825,
        longitude=30.7233,
        sog=12.5,
        cog=135.0,
        nav_status=0,
    )


def static_record():
    now = epoch_micros()
    return ais_ship_static_pb2.AisShipStatic(
        event_id=f"test-{uuid.uuid4()}",
        message_type="ShipStaticData",
        mmsi=TEST_MMSI,
        event_ts=now,
        ingest_ts=now,
        producer_id=PRODUCER_ID,
        producer_seq=1,
        report_part="full",
        ship_name="TEST VESSEL",
        ship_type=70,
        destination="ODESSA",
    )


def send_one(sdk, table, record, client_id, client_secret):
    properties = TableProperties(table, type(record).DESCRIPTOR)
    stream = sdk.create_stream(client_id, client_secret, properties)
    try:
        offset = stream.ingest_record_offset(record)
        started = time.monotonic()
        stream.wait_for_offset(offset)
        logger.info("%s: acknowledged at offset %s in %.0f ms", table, offset, (time.monotonic() - started) * 1000)
    finally:
        stream.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()

    endpoint = os.environ["ZEROBUS_ENDPOINT"]
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    schema = f"{os.environ['ZEROBUS_CATALOG']}.{os.environ['ZEROBUS_SCHEMA']}"

    sdk = ZerobusSdk(endpoint, workspace_url)
    send_one(sdk, f"{schema}.bronze_positions", position_record(), client_id, client_secret)
    send_one(sdk, f"{schema}.bronze_ship_static", static_record(), client_id, client_secret)


if __name__ == "__main__":
    main()