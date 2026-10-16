import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PRODUCER_DIR = Path(__file__).resolve().parent
PROTO_DIR = PRODUCER_DIR / "proto"
ENV_FILE = PRODUCER_DIR.parents[1] / ".env"

SCHEMAS = {
    "bronze_positions": ("ais_position.proto", "AisPosition"),
    "bronze_ship_static": ("ais_ship_static.proto", "AisShipStatic"),
}

logger = logging.getLogger("build_proto")


def require(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Environment variable {name} is not set")
    return value


def run(args):
    subprocess.run([sys.executable, "-m", *args], check=True)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv(ENV_FILE)
    schema = f"{require('ZEROBUS_CATALOG')}.{require('ZEROBUS_SCHEMA')}"

    for table, (file_name, message) in SCHEMAS.items():
        output = PROTO_DIR / file_name
        run([
            "zerobus.tools.generate_proto",
            "--uc-endpoint", require("DATABRICKS_WORKSPACE_URL"),
            "--client-id", require("DATABRICKS_CLIENT_ID"),
            "--client-secret", require("DATABRICKS_CLIENT_SECRET"),
            "--table", f"{schema}.{table}",
            "--output", str(output),
            "--proto-msg", message,
        ])
        logger.info("Generated %s from %s.%s", output.name, schema, table)

    run([
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={PROTO_DIR}",
        *(str(PROTO_DIR / file_name) for file_name, _ in SCHEMAS.values()),
    ])
    logger.info("Compiled Python modules into %s", PROTO_DIR)


if __name__ == "__main__":
    main()