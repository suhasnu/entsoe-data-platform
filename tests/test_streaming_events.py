import json

import pandas as pd

from gridflow.streaming.events import events_from_frame


def frame(**overrides: object) -> pd.DataFrame:
    base = {
        "zone_code": ["DE_LU", "DE_LU"],
        "ts_utc": pd.to_datetime(["2026-03-01T00:00:00Z", "2026-03-01T00:15:00Z"]),
        "load_mw": [48000.0, 47850.0],
        "resolution_minutes": [15, 15],
        "run_id": ["abc", "abc"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_one_event_per_row_keyed_by_zone() -> None:
    events = events_from_frame(frame(), "entsoe_load", ("load_mw",))

    assert len(events) == 2
    assert {event.key for event in events} == {b"DE_LU"}
    assert events[0].payload == {"load_mw": 48000.0}


def test_event_id_is_stable_across_runs() -> None:
    first = events_from_frame(frame(), "entsoe_load", ("load_mw",))
    second = events_from_frame(frame(), "entsoe_load", ("load_mw",))

    assert [e.event_id for e in first] == [e.event_id for e in second]
    assert len({e.event_id for e in first}) == 2


def test_event_id_changes_with_event_type() -> None:
    load = events_from_frame(frame(), "entsoe_load", ("load_mw",))
    other = events_from_frame(frame(), "entsoe_generation", ("load_mw",))

    assert load[0].event_id != other[0].event_id


def test_missing_measure_serialises_as_null() -> None:
    events = events_from_frame(frame(load_mw=[float("nan"), 47850.0]), "entsoe_load", ("load_mw",))

    assert events[0].payload["load_mw"] is None
    assert json.loads(events[0].to_json())["payload"]["load_mw"] is None


def test_event_time_is_kept_apart_from_processing_time() -> None:
    event = events_from_frame(frame(), "entsoe_load", ("load_mw",))[0]

    assert event.ts_utc.startswith("2026-03-01T00:00:00")
    assert event.produced_at > event.ts_utc
