import asyncio
import logging
import threading
import time
from collections import Counter

logger = logging.getLogger("producer.metrics")


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counters = Counter()
        self._lag_micros_total = 0
        self._lag_samples = 0
        self._started = time.monotonic()

    def increment(self, name, value=1):
        with self._lock:
            self._counters[name] += value

    def observe_lag(self, event_ts, ingest_ts):
        with self._lock:
            self._lag_micros_total += ingest_ts - event_ts
            self._lag_samples += 1

    def snapshot(self):
        with self._lock:
            counters = dict(self._counters)
            lag_ms = self._lag_micros_total / self._lag_samples / 1000 if self._lag_samples else 0
        counters["elapsed_s"] = time.monotonic() - self._started
        counters["avg_source_lag_ms"] = lag_ms
        return counters

    def log(self, label):
        s = self.snapshot()
        elapsed = max(s["elapsed_s"], 1e-9)
        sent = s.get("sent_positions", 0) + s.get("sent_static", 0)
        acked = s.get("acked_positions", 0) + s.get("acked_static", 0)
        failed = s.get("failed_positions", 0) + s.get("failed_static", 0)
        logger.info(
            "%s: received=%d skipped=%d malformed=%d sent=%d (positions=%d static=%d) acked=%d failed=%d "
            "in_flight=%d replayed=%d reconnects=%d rate=%.1f rec/s source_lag=%.0f ms elapsed=%.0f s",
            label,
            s.get("received", 0),
            s.get("skipped", 0),
            s.get("malformed", 0),
            sent,
            s.get("sent_positions", 0),
            s.get("sent_static", 0),
            acked,
            failed,
            sent - acked - failed,
            s.get("replayed", 0),
            s.get("reconnects", 0),
            sent / elapsed,
            s["avg_source_lag_ms"],
            elapsed,
        )

    async def report_every(self, interval):
        while True:
            await asyncio.sleep(interval)
            self.log("progress")