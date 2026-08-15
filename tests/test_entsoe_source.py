import pandas as pd

from gridflow.ingestion.entsoe import _with_resolution


def test_quarter_hourly_resolution_detected() -> None:
    stamps = pd.date_range("2026-08-01", periods=96, freq="15min", tz="UTC")
    frame = pd.DataFrame({"ts_utc": stamps})
    assert _with_resolution(frame)["resolution_minutes"].unique().tolist() == [15]


def test_hourly_resolution_detected() -> None:
    frame = pd.DataFrame({"ts_utc": pd.date_range("2026-08-01", periods=24, freq="h", tz="UTC")})
    assert _with_resolution(frame)["resolution_minutes"].unique().tolist() == [60]


def test_repeated_timestamps_do_not_break_detection() -> None:
    """Melted generation frames repeat each timestamp once per production type."""
    stamps = pd.date_range("2026-08-01", periods=96, freq="15min", tz="UTC")
    frame = pd.DataFrame({"ts_utc": stamps.repeat(16)})
    assert _with_resolution(frame)["resolution_minutes"].unique().tolist() == [15]


def test_single_row_falls_back_to_hourly() -> None:
    frame = pd.DataFrame({"ts_utc": pd.to_datetime(["2026-08-01T00:00:00Z"])})
    assert _with_resolution(frame)["resolution_minutes"].unique().tolist() == [60]
