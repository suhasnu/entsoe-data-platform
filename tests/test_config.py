import pytest
from pydantic import ValidationError

from gridflow.config import Settings


def test_zones_parsed_from_csv(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    monkeypatch.setenv("ZONES", "DE_LU, AT ,NL")
    settings = Settings(_env_file=None)
    assert settings.zones == ["DE_LU", "AT", "NL"]


def test_missing_api_key_fails(monkeypatch):
    monkeypatch.delenv("ENTSOE_API_KEY", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_database_url_format(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    settings = Settings(_env_file=None)
    assert settings.database_url.startswith("postgresql+psycopg2://")
    assert ":5433/" in settings.database_url