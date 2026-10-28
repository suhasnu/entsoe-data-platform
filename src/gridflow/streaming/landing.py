from pathlib import Path
from typing import Any

import pandas as pd

PARTITION_COLUMNS = ["zone_code", "event_date"]


def _flatten(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    return {key: value for key, value in event.items() if key != "payload"} | payload


def to_frame(events: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame([_flatten(event) for event in events])
    frame["ts_utc"] = pd.to_datetime(frame["ts_utc"], utc=True)
    frame["produced_at"] = pd.to_datetime(frame["produced_at"], utc=True)
    frame["event_date"] = frame["ts_utc"].dt.strftime("%Y-%m-%d")
    return frame


def write_batch(events: list[dict[str, Any]], root: Path) -> int:
    """Append one batch as Parquet, partitioned by zone and event date.

    Nothing is deduplicated here. At-least-once delivery means a redelivered
    batch lands as a second file, and read_landing is what resolves it.
    """
    frame = to_frame(events)
    root.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(root, partition_cols=PARTITION_COLUMNS, index=False)
    return len(frame)


def read_landing(root: Path) -> pd.DataFrame:
    """Read the landing zone, keeping the most recently produced copy of each event.

    Same rule as the bronze layer in BigQuery, where staging ranks on
    ingested_at, so both paths resolve a redelivery the same way.
    """
    frame = pd.read_parquet(root)
    return (
        frame.sort_values("produced_at")
        .drop_duplicates(subset="event_id", keep="last")
        .sort_values(["zone_code", "ts_utc"])
        .reset_index(drop=True)
    )
