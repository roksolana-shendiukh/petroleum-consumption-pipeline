import os
import pytest
from databricks.connect import DatabricksSession

DEFAULT_CLUSTER_ID = "0828-065013-gln8ogbm"


@pytest.fixture(scope="session")
def spark():
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