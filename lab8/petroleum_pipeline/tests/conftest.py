import os

import pytest

DEFAULT_CLUSTER_ID = "0828-065013-gln8ogbm"


def _local_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _databricks_session():
    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder

    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return builder.getOrCreate()

    if os.getenv("DATABRICKS_HOST"):
        if not os.getenv("DATABRICKS_CLUSTER_ID"):
            builder = builder.serverless()
        return builder.getOrCreate()

    return builder.profile(os.getenv("DATABRICKS_PROFILE", "dev")).clusterId(
        os.getenv("DATABRICKS_CLUSTER_ID", DEFAULT_CLUSTER_ID)
    ).getOrCreate()


@pytest.fixture(scope="session")
def spark():
    if os.getenv("USE_LOCAL_SPARK"):
        return _local_session()
    return _databricks_session()