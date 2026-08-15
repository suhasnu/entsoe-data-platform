import uuid
from datetime import date, datetime

import pandas as pd
import pytest
from pandera.pandas import DataFrameSchema

from gridflow.ingestion.base import (
    PermanentSourceError,
    RateLimiter,
    Source,
    TransientSourceError,
)

PERMISSIVE = DataFrameSchema({}, strict=False, coerce=True)


class FlakySource(Source):
    name = "flaky"
    schema = PERMISSIVE

    def __init__(self, fail_times: int) -> None:
        super().__init__(rate_limiter=RateLimiter(calls_per_minute=100_000))
        self.fail_times = fail_times
        self.attempts = 0

    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise TransientSourceError("upstream hiccup")
        return pd.DataFrame({"ts_utc": [start], "value": [1.0]})


class BrokenSource(Source):
    name = "broken"
    schema = PERMISSIVE

    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        raise PermanentSourceError("bad credentials")


def test_transient_errors_are_retried() -> None:
    source = FlakySource(fail_times=2)
    result = source.ingest("DE_LU", date(2025, 6, 15))
    assert source.attempts == 3
    assert len(result.frame) == 1


def test_permanent_errors_are_not_retried() -> None:
    source = BrokenSource()
    with pytest.raises(PermanentSourceError):
        source.ingest("DE_LU", date(2025, 6, 15))


def test_zone_and_run_id_are_attached() -> None:
    source = FlakySource(fail_times=0)
    run_id = uuid.uuid4()
    result = source.ingest("DE_LU", date(2025, 6, 15), run_id)
    assert result.frame["zone_code"].unique().tolist() == ["DE_LU"]
    assert result.frame["run_id"].unique().tolist() == [str(run_id)]
