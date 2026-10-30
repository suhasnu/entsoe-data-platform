import pandas as pd

from gridflow.streaming.compare import compare


def hours(zone: str, start: str, count: int, load: float, intervals: int) -> pd.DataFrame:
    stamps = pd.date_range(start, periods=count, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "zone_code": zone,
            "ts_hour_utc": stamps,
            "load_mw": load,
            "intervals": intervals,
        }
    )


def batch_frame(**kwargs: object) -> pd.DataFrame:
    frame = hours(**kwargs)  # type: ignore[arg-type]
    return frame.rename(columns={"load_mw": "load_mw_batch", "intervals": "intervals_batch"})


def stream_frame(**kwargs: object) -> pd.DataFrame:
    frame = hours(**kwargs)  # type: ignore[arg-type]
    return frame.rename(columns={"load_mw": "load_mw_stream", "intervals": "intervals_stream"})


def test_identical_paths_agree() -> None:
    batch = batch_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=24, load=48000.0, intervals=4
    )
    stream = stream_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=24, load=48000.0, intervals=4
    )

    _, summary = compare(batch, stream)

    assert summary["hours_in_both"] == 24
    assert summary["batch_only"] == 0
    assert summary["stream_only"] == 0
    assert summary["load_disagreements"] == 0
    assert summary["max_abs_delta_mw"] == 0.0


def test_watermark_holding_back_hours_shows_as_batch_only() -> None:
    batch = batch_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=24, load=48000.0, intervals=4
    )
    stream = stream_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=22, load=48000.0, intervals=4
    )

    _, summary = compare(batch, stream)

    assert summary["batch_only"] == 2
    assert summary["stream_only"] == 0
    assert summary["hours_in_both"] == 22


def test_a_stale_value_on_one_side_is_reported() -> None:
    batch = batch_frame(zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=48000.0, intervals=4)
    stream = stream_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=47000.0, intervals=4
    )

    merged, summary = compare(batch, stream)

    assert summary["load_disagreements"] == 3
    assert summary["max_abs_delta_mw"] == 1000.0
    assert (merged["delta_mw"] == 1000.0).all()


def test_tolerance_absorbs_float_noise() -> None:
    batch = batch_frame(zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=48000.0, intervals=4)
    stream = stream_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=48000.2, intervals=4
    )

    _, summary = compare(batch, stream)

    assert summary["load_disagreements"] == 0


def test_interval_counts_are_compared_separately_from_values() -> None:
    batch = batch_frame(zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=48000.0, intervals=4)
    stream = stream_frame(
        zone="DE_LU", start="2026-10-26T00:00Z", count=3, load=48000.0, intervals=3
    )

    _, summary = compare(batch, stream)

    assert summary["load_disagreements"] == 0
    assert summary["interval_disagreements"] == 3
