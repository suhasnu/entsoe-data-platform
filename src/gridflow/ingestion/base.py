import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from gridflow.logging import get_logger
from gridflow.timewindows import berlin_day_window

log = get_logger(__name__)


class TransientSourceError(Exception):
    """Upstream failure worth retrying: timeout, 5xx, rate limit."""


class PermanentSourceError(Exception):
    """Upstream failure retrying will not fix: bad credentials, no data."""


@dataclass(frozen=True)
class IngestResult:
    run_id: uuid.UUID
    source: str
    zone_code: str
    frame: pd.DataFrame


class RateLimiter:
    """Token bucket. ENTSO-E bans for ten minutes above 400 requests a minute."""

    def __init__(self, calls_per_minute: int = 200) -> None:
        self._interval = 60.0 / calls_per_minute
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self) -> None:
        with self._lock:
            wait = self._interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


class Source(ABC):
    name: str

    def __init__(self, rate_limiter: RateLimiter | None = None) -> None:
        self._limiter = rate_limiter or RateLimiter()

    @abstractmethod
    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Call the upstream API. Raise Transient or PermanentSourceError."""

    @retry(
        retry=retry_if_exception_type(TransientSourceError),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=2, max=60),
        reraise=True,
    )
    def _fetch_with_retry(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        self._limiter.acquire()
        return self.fetch(zone_code, start, end)

    def ingest(self, zone_code: str, day: date, run_id: uuid.UUID | None = None) -> IngestResult:
        run_id = run_id or uuid.uuid4()
        start, end = berlin_day_window(day)

        log.info("ingest.start", source=self.name, zone=zone_code, day=str(day))
        frame = self._fetch_with_retry(zone_code, start, end)
        frame = frame.assign(zone_code=zone_code, run_id=str(run_id))
        log.info("ingest.done", source=self.name, zone=zone_code, rows=len(frame))

        return IngestResult(run_id, self.name, zone_code, frame)