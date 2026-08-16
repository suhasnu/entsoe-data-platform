from fastapi.testclient import TestClient

from gridflow.api.main import app

client = TestClient(app)


def test_health_needs_no_auth() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_zones_requires_api_key() -> None:
    assert client.get("/v1/zones").status_code == 422


def test_invalid_api_key_rejected() -> None:
    response = client.get("/v1/zones", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


def test_malformed_zone_rejected() -> None:
    response = client.get(
        "/v1/grid/hourly", params={"zone": "not-a-zone"}, headers={"X-API-Key": "local-dev-key"}
    )
    assert response.status_code == 422
