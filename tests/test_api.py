import pytest
from fastapi.testclient import TestClient

from gridflow.api.main import app
from gridflow.config import get_settings

client = TestClient(app)


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> str:
    """Tests must not depend on whatever happens to be in the local .env."""
    key = "test-key"
    monkeypatch.setattr(get_settings(), "api_keys_raw", key)
    return key


def test_health_needs_no_auth() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_missing_api_key_rejected() -> None:
    assert client.get("/v1/zones").status_code == 422


def test_invalid_api_key_rejected(api_key: str) -> None:
    response = client.get("/v1/zones", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


def test_malformed_zone_rejected(api_key: str) -> None:
    response = client.get(
        "/v1/grid/hourly", params={"zone": "not-a-zone"}, headers={"X-API-Key": api_key}
    )
    assert response.status_code == 422
