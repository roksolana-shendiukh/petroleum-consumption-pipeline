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
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--table")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    config = load_config(args.config)
    catalog = args.catalog or config["catalog"]
    schema = args.schema or config["schema"]
    table = args.table or config["table"]
    full_name = f"{catalog}.{schema}.{table}"

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