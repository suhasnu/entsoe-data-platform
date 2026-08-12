import pandas as pd
import pandera.errors
import pytest

from gridflow.quality.schemas import GENERATION_SCHEMA, LOAD_SCHEMA, PRICE_SCHEMA


def load_frame(**overrides: object) -> pd.DataFrame:
    base = {
        "zone_code": ["DE_LU"],
        "ts_utc": pd.to_datetime(["2026-08-01T10:00:00Z"]),
        "load_mw": [55_000.0],
        "resolution_minutes": [15],
        "run_id": ["11111111-1111-1111-1111-111111111111"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_valid_row_passes() -> None:
    assert len(LOAD_SCHEMA.validate(load_frame())) == 1


def test_implausible_load_is_rejected() -> None:
    with pytest.raises(pandera.errors.SchemaError):
        LOAD_SCHEMA.validate(load_frame(load_mw=[900_000.0]))


def test_null_measure_allowed_but_null_timestamp_is_not() -> None:
    LOAD_SCHEMA.validate(load_frame(load_mw=[None]))
    with pytest.raises(pandera.errors.SchemaError):
        LOAD_SCHEMA.validate(load_frame(ts_utc=[pd.NaT]))


def test_unexpected_resolution_is_rejected() -> None:
    with pytest.raises(pandera.errors.SchemaError):
        LOAD_SCHEMA.validate(load_frame(resolution_minutes=[30]))


def test_negative_prices_are_valid() -> None:
    frame = pd.DataFrame(
        {
            "zone_code": ["DE_LU"],
            "ts_utc": pd.to_datetime(["2026-05-01T13:00:00Z"]),
            "price_eur_mwh": [-42.5],
            "resolution_minutes": [60],
            "run_id": ["11111111-1111-1111-1111-111111111111"],
        }
    )
    assert len(PRICE_SCHEMA.validate(frame)) == 1


def test_undeclared_columns_are_filtered_out() -> None:
    frame = load_frame()
    frame["unexpected"] = ["surprise"]
    assert "unexpected" not in LOAD_SCHEMA.validate(frame).columns


def test_duplicate_production_type_is_rejected() -> None:
    """Guards the pumped storage collapse: two rows for one type and timestamp."""
    frame = pd.DataFrame(
        {
            "zone_code": ["DE_LU", "DE_LU"],
            "ts_utc": pd.to_datetime(["2026-08-01T10:00:00Z", "2026-08-01T10:00:00Z"]),
            "production_type": ["Hydro Pumped Storage", "Hydro Pumped Storage"],
            "generation_mw": [100.0, 200.0],
            "resolution_minutes": [15, 15],
            "run_id": ["1", "1"],
        }
    )
    with pytest.raises(pandera.errors.SchemaError):
        GENERATION_SCHEMA.validate(frame)