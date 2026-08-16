from datetime import date, datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class Zone(BaseModel):
    zone_code: str
    zone_name: str
    country: str
    timezone: str


class GridHour(BaseModel):
    zone_code: str
    ts_hour_utc: datetime
    date_local: date
    load_mw: float | None = None
    renewable_mw: float | None = None
    fossil_mw: float | None = None
    nuclear_mw: float | None = None
    total_generation_mw: float | None = None
    renewable_pct: float | None = Field(None, ge=0, le=100)
    residual_load_mw: float | None = None
    carbon_intensity_g_kwh: float | None = None
    price_eur_mwh: float | None = None


class Page(BaseModel, Generic[T]):
    items: list[T]
    count: int
    next_cursor: str | None = None
