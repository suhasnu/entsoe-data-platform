import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "gridflow.streaming")


@dataclass(frozen=True)
class GridEvent:
    """One measurement, one zone, one timestamp.

    Event time and processing time are separate fields on purpose. ENTSO-E
    publishes a whole market day in one response, so a poll at noon carries
    events stamped from midnight onwards. Anything that windows on arrival
    would drop the entire day into a single bucket.
    """

    event_id: str
    event_type: str
    zone_code: str
    ts_utc: str
    produced_at: str
    resolution_minutes: int
    payload: dict[str, Any]

    @property
    def key(self) -> bytes:
        return self.zone_code.encode()

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()


def event_id(event_type: str, zone_code: str, ts_utc: str) -> str:
    """Deterministic id, so re-publishing a day a consumer already saw is a no-op."""
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{event_type}|{zone_code}|{ts_utc}"))


def _measure(value: Any) -> float | None:
    # NaN is valid in a DataFrame and invalid in JSON.
    if value is None or pd.isna(value):
        return None
    return float(value)


def events_from_frame(
    frame: pd.DataFrame, event_type: str, measures: tuple[str, ...]
) -> list[GridEvent]:
    """Fan a validated ingestion frame out into one event per row.

    This is the seam where the batch source becomes a stream, and the only
    place in the project that pretends a REST response is a feed.
    """
    produced_at = datetime.now(UTC).isoformat()

    events = []
    for record in frame.to_dict("records"):
        zone_code = str(record["zone_code"])
        ts_utc = pd.Timestamp(record["ts_utc"]).tz_convert("UTC").isoformat()
        events.append(
            GridEvent(
                event_id=event_id(event_type, zone_code, ts_utc),
                event_type=event_type,
                zone_code=zone_code,
                ts_utc=ts_utc,
                produced_at=produced_at,
                resolution_minutes=int(record["resolution_minutes"]),
                payload={name: _measure(record[name]) for name in measures},
            )
        )
    return events
