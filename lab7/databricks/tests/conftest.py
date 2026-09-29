import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.profile("dev").clusterId("0828-065013-gln8ogbm").getOrCreate()

