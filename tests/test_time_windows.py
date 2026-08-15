from datetime import UTC, date

import pytest

from gridflow.timewindows import MARKET_TZ, berlin_day_window


@pytest.mark.parametrize(
    ("day", "expected_hours"),
    [
        ("2025-03-30", 23),
        ("2025-10-26", 25),
        ("2025-06-15", 24),
        ("2025-01-15", 24),
        ("2026-03-29", 23),
        ("2026-10-25", 25),
    ],
)
def test_day_length_handles_dst(day: str, expected_hours: int) -> None:
    start, end = berlin_day_window(date.fromisoformat(day))
    assert (end - start).total_seconds() / 3600 == expected_hours


def test_window_starts_and_ends_at_local_midnight() -> None:
    start, end = berlin_day_window(date(2025, 6, 15))
    assert start.astimezone(MARKET_TZ).hour == 0
    assert end.astimezone(MARKET_TZ).hour == 0


def test_window_is_returned_in_utc() -> None:
    start, end = berlin_day_window(date(2025, 6, 15))
    assert start.tzinfo == UTC
    assert end.tzinfo == UTC
