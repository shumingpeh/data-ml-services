import pytest


class MockSettings:
    DATABRICKS_CLUSTER_HOST = "https://random.cloud.databricks.com"
    DATABRICKS_PAT_TOKEN = "test"  # noqa: S105 pragma: allowlist secret
    PIPELINE_TYPE = "TRIGGERED"
    UNITY_CATALOG = "testing.schema"
    VS_ENDPOINT_TYPE = "STANDARD"
    DATABRICKS_SQL_CLUSTER_PATH = "sql_path"


@pytest.fixture(autouse=True)
def dummy_settings():
    return MockSettings()


@pytest.fixture(autouse=True)
def dummy_list_endpoints():
    return {"endpoints": [{"name": "testing"}]}


@pytest.fixture(autouse=True)
def dummy_list_endpoints_not_exist():
    return {"endpoints": [{"name": "test"}]}


@pytest.fixture(autouse=True)
def dummy_get_endpoint():
    return {
        "name": "testing",
        "endpoint_type": "STANDARD",
        "id": "test-123",
        "endpoint_status": {"state": "ONLINE"},
        "num_indexes": 1,
    }


@pytest.fixture(autouse=True)
def dummy_list_indexes():
    return {"vector_indexes": [{"name": "test.index"}]}


@pytest.fixture(autouse=True)
def dummy_get_index():
    return {
        "name": "test.index",
        "endpoint_name": "testing",
        "primary_key": "id",
        "pipeline_type": "TRIGGERED",
        "status": {"detailed_state": "ONLINE_NO_PENDING_UPDATE"},
        "endpoint_type": "STANDARD",
    }
