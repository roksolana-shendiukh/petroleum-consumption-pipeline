import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

PRODUCER_DIR = Path(__file__).resolve().parent
ENV_FILE = PRODUCER_DIR.parents[1] / ".env"
CONFIG_FILE = PRODUCER_DIR / "producer_config.yaml"


@dataclass(frozen=True)
class Settings:
    aisstream_api_key: str
    workspace_url: str
    zerobus_endpoint: str
    client_id: str
    client_secret: str
    catalog: str
    schema: str
    region: str
    bounding_boxes: list
    duration_seconds: int
    producer_id: str
    metrics_interval_seconds: int
    reconnect_initial_backoff_seconds: float
    reconnect_max_backoff_seconds: float
    connection_per_stream: bool
    max_inflight_records: int
    ingest_retries: int
    retry_backoff_seconds: float

    @property
    def positions_table(self):
        return f"{self.catalog}.{self.schema}.bronze_positions"

    @property
    def static_table(self):
        return f"{self.catalog}.{self.schema}.bronze_ship_static"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="producer")
    parser.add_argument("--config", type=Path, default=CONFIG_FILE)
    parser.add_argument("--region", help="overrides the region from the config file")
    parser.add_argument("--duration", type=int, default=0, help="seconds to run, 0 runs until Ctrl+C")
    parser.add_argument("--producer-name", default="producer-1")
    return parser.parse_args(argv)


def require(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Environment variable {name} is not set")
    return value


def load_config(path):
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_settings(argv=None):
    load_dotenv(ENV_FILE)
    args = parse_args(argv)
    config = load_config(args.config)

    region = args.region or config["region"]
    if region not in config["regions"]:
        raise SystemExit(f"Unknown region '{region}', available: {', '.join(config['regions'])}")

    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Settings(
        aisstream_api_key=require("AISSTREAM_API_KEY"),
        workspace_url=require("DATABRICKS_WORKSPACE_URL"),
        zerobus_endpoint=require("ZEROBUS_ENDPOINT"),
        client_id=require("DATABRICKS_CLIENT_ID"),
        client_secret=require("DATABRICKS_CLIENT_SECRET"),
        catalog=require("ZEROBUS_CATALOG"),
        schema=require("ZEROBUS_SCHEMA"),
        region=region,
        bounding_boxes=config["regions"][region],
        duration_seconds=args.duration,
        producer_id=f"{args.producer_name}-{started}",
        metrics_interval_seconds=config["metrics_interval_seconds"],
        reconnect_initial_backoff_seconds=config["source"]["reconnect_initial_backoff_seconds"],
        reconnect_max_backoff_seconds=config["source"]["reconnect_max_backoff_seconds"],
        connection_per_stream=config["sink"]["connection_per_stream"],
        max_inflight_records=config["sink"]["max_inflight_records"],
        ingest_retries=config["sink"]["ingest_retries"],
        retry_backoff_seconds=config["sink"]["retry_backoff_seconds"],
    )