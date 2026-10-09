import asyncio
import logging
import time

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    NonRetriableException,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)

from .normalizer import POSITIONS, STATIC
from .proto import ais_position_pb2, ais_ship_static_pb2

APPLICATION_NAME = "ais-zerobus-producer/1.0"

MESSAGES = {
    POSITIONS: ais_position_pb2.AisPosition,
    STATIC: ais_ship_static_pb2.AisShipStatic,
}

logger = logging.getLogger("producer.sink")


class TableAckCallback(AckCallback):
    def __init__(self, kind, metrics):
        super().__init__()
        self.kind = kind
        self.metrics = metrics

    def on_ack(self, offset):
        self.metrics.increment(f"acked_{self.kind}")

    def on_error(self, offset, error_message):
        self.metrics.increment(f"failed_{self.kind}")
        logger.error("%s: record at offset %s failed: %s", self.kind, offset, error_message)


class ZerobusSink:
    def __init__(self, settings, metrics):
        self.settings = settings
        self.metrics = metrics
        self.sdk = ZerobusSdk(
            settings.zerobus_endpoint,
            settings.workspace_url,
            application_name=APPLICATION_NAME,
            connection_per_stream=settings.connection_per_stream,
        )
        self.tables = {POSITIONS: settings.positions_table, STATIC: settings.static_table}
        self.streams = {}
        self.sequences = {POSITIONS: 0, STATIC: 0}

    async def open(self):
        for kind, table in self.tables.items():
            options = StreamConfigurationOptions(
                max_inflight_records=self.settings.max_inflight_records,
                recovery=True,
                ack_callback=TableAckCallback(kind, self.metrics),
            )
            properties = TableProperties(table, MESSAGES[kind].DESCRIPTOR)
            self.streams[kind] = await self.sdk.create_stream(
                self.settings.client_id, self.settings.client_secret, properties, options
            )
            logger.info("Opened stream to %s", table)

    def build(self, kind, fields):
        self.sequences[kind] += 1
        ingest_ts = time.time_ns() // 1000
        self.metrics.observe_lag(fields["event_ts"], ingest_ts)
        values = {key: value for key, value in fields.items() if value is not None}
        values.update(ingest_ts=ingest_ts, producer_id=self.settings.producer_id, producer_seq=self.sequences[kind])
        return MESSAGES[kind](**values)

    async def send(self, kind, fields):
        record = self.build(kind, fields)
        retries = self.settings.ingest_retries
        for attempt in range(1, retries + 1):
            try:
                await self.streams[kind].ingest_record_offset(record)
                self.metrics.increment(f"sent_{kind}")
                return
            except NonRetriableException:
                logger.exception("%s: non-retriable error, stopping", kind)
                raise
            except ZerobusException as error:
                logger.warning("%s: ingest failed (attempt %d/%d): %s", kind, attempt, retries, error)
                if attempt == retries:
                    raise
                await self.recreate(kind)
                await asyncio.sleep(self.settings.retry_backoff_seconds * attempt)

    async def recreate(self, kind):
        stream = self.streams[kind]
        try:
            unacked = len(list(await stream.get_unacked_records()))
            self.streams[kind] = await self.sdk.recreate_stream(stream)
        except ZerobusException as error:
            logger.warning("%s: stream is still active, retrying on it (%s)", kind, error)
            return
        self.metrics.increment("replayed", unacked)
        logger.warning("%s: stream recreated, %d unacknowledged records replayed", kind, unacked)

    async def close(self):
        for kind, stream in self.streams.items():
            try:
                await stream.flush()
                await stream.close()
                logger.info("Closed stream to %s", self.tables[kind])
            except ZerobusException:
                logger.exception("%s: failed to flush and close the stream", kind)