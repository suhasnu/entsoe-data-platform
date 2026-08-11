from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("Europe/Berlin")


def berlin_day_window(day: date) -> tuple[datetime, datetime]:
    """Local midnight to local midnight for a market day, returned in UTC.

    Spans 23 hours on the spring clock change and 25 on the autumn one.
    The window is returned in UTC because subtracting two datetimes that
    share a tzinfo is done as naive arithmetic, which silently ignores the
    offset change.
    """
    start = datetime.combine(day, datetime.min.time(), tzinfo=MARKET_TZ)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=MARKET_TZ)
    return start.astimezone(UTC), end.astimezone(UTC)