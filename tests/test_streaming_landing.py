from pathlib import Path

import pandas as pd

from gridflow.streaming.landing import read_landing, write_batch


def event(zone: str, hour: int, produced_at: str, load_mw: float = 48000.0) -> dict[str, object]:
    ts = f"2026-03-01T{hour:02d}:00:00+00:00"
    return {
        "event_id": f"{zone}-{hour}",
        "event_type": "entsoe_load",
        "zone_code": zone,
        "ts_utc": ts,
        "produced_at": produced_at,
        "resolution_minutes": 60,
        "payload": {"load_mw": load_mw},
    }


def test_payload_is_flattened_into_columns(tmp_path: Path) -> None:
    write_batch([event("DE_LU", 0, "2026-03-03T10:00:00+00:00")], tmp_path)
    frame = read_landing(tmp_path)

    assert "load_mw" in frame.columns
    assert "payload" not in frame.columns
    assert frame["load_mw"].iloc[0] == 48000.0


def test_partitioned_by_zone_and_event_date(tmp_path: Path) -> None:
    write_batch(
        [
            event("DE_LU", 0, "2026-03-03T10:00:00+00:00"),
            event("FR", 0, "2026-03-03T10:00:00+00:00"),
        ],
        tmp_path,
    )

    assert (tmp_path / "zone_code=DE_LU" / "event_date=2026-03-01").is_dir()
    assert (tmp_path / "zone_code=FR" / "event_date=2026-03-01").is_dir()


def test_redelivery_collapses_to_the_latest_copy(tmp_path: Path) -> None:
    write_batch([event("DE_LU", 0, "2026-03-03T10:00:00+00:00", load_mw=48000.0)], tmp_path)
    write_batch([event("DE_LU", 0, "2026-03-03T11:00:00+00:00", load_mw=48500.0)], tmp_path)

    raw = pd.read_parquet(tmp_path)
    assert len(raw) == 2

    frame = read_landing(tmp_path)
    assert len(frame) == 1
    assert frame["load_mw"].iloc[0] == 48500.0


def test_rows_come_back_ordered_within_a_zone(tmp_path: Path) -> None:
    write_batch(
        [
            event("DE_LU", 5, "2026-03-03T10:00:00+00:00"),
            event("DE_LU", 1, "2026-03-03T10:00:00+00:00"),
        ],
        tmp_path,
    )
    frame = read_landing(tmp_path)

    assert frame["ts_utc"].is_monotonic_increasing
