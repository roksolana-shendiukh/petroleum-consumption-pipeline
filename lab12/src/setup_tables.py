import argparse
import logging
import sys

from pyspark.sql import SparkSession

from lab12_tables.definitions import create_table_ddl, full_name, load_tables

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables-config", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)
    tables = load_tables(args.tables_config)
    spark = SparkSession.builder.getOrCreate()
    for table in tables.values():
        name = full_name(args.catalog, args.schema, table)
        spark.sql(create_table_ddl(name, table))
        logger.info("Table ready: %s", name)


if __name__ == "__main__":
    main(sys.argv[1:])