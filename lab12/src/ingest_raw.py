from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from eia_ingest.config import load_config
from eia_ingest.downloader import download
from eia_ingest.http_client import Page, create_session
from eia_ingest.storage import save_raw
from eia_ingest.targets import build_targets


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    config = load_config(args.config)
    full_name = f"{args.catalog}.{args.schema}.{args.table}"

    spark = SparkSession.builder.getOrCreate()
    run_ts = datetime.now(timezone.utc)
    cache: dict[str, Page | None] = {}

    with create_session(config["http"]) as session:
        targets = build_targets(session, config, cache)
        rows, failed = download(session, targets, config, run_ts, cache)

    if failed:
        raise RuntimeError(f"{len(failed)} pages failed: {failed}")

    save_raw(spark, rows, full_name)


if __name__ == "__main__":
    main(sys.argv[1:])