from datetime import datetime

import pandas as pd
import requests
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

from gridflow.config import get_settings
from gridflow.ingestion.base import PermanentSourceError, Source, TransientSourceError
from gridflow.quality.schemas import GENERATION_SCHEMA, LOAD_SCHEMA, PRICE_SCHEMA

ZONE_TO_AREA = {
    "DE_LU": "DE_LU",
    "AT": "AT",
    "NL": "NL",
    "FR": "FR",
    "DK_1": "DK_1",
    "DK_2": "DK_2",
}


def _client() -> EntsoePandasClient:
    return EntsoePandasClient(api_key=get_settings().entsoe_api_key)


def _classify(exc: Exception) -> Exception:
    """Only retry what retrying can fix."""
    if isinstance(exc, NoMatchingDataError):
        return PermanentSourceError(f"no data for the requested window: {exc}")
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", 0)
        if status == 429 or status >= 500:
            return TransientSourceError(f"HTTP {status}")
        return PermanentSourceError(f"HTTP {status}")
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return TransientSourceError(str(exc))
    return exc


def _to_utc(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return index.tz_convert("UTC") if index.tz is not None else index.tz_localize("UTC")


def _with_resolution(frame: pd.DataFrame) -> pd.DataFrame:
    """Zones publish at 15 or 60 minutes, so each row records its own grain."""
    deltas = frame["ts_utc"].drop_duplicates().sort_values().diff().dropna()
    minutes = int(deltas.mode().iloc[0].total_seconds() // 60) if len(deltas) else 60
    return frame.assign(resolution_minutes=minutes)


class EntsoeLoadSource(Source):
    name = "entsoe_load"
    schema = LOAD_SCHEMA

    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        try:
            raw = _client().query_load(
                ZONE_TO_AREA[zone_code], start=pd.Timestamp(start), end=pd.Timestamp(end)
            )
        except Exception as exc:
            raise _classify(exc) from exc

        series = raw.iloc[:, 0] if isinstance(raw, pd.DataFrame) else raw
        frame = series.rename("load_mw").to_frame()
        frame.index = _to_utc(frame.index)
        return _with_resolution(frame.rename_axis("ts_utc").reset_index())


class EntsoeGenerationSource(Source):
    name = "entsoe_generation"
    schema = GENERATION_SCHEMA

    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        try:
            raw = _client().query_generation(
                ZONE_TO_AREA[zone_code], start=pd.Timestamp(start), end=pd.Timestamp(end)
            )
        except Exception as exc:
            raise _classify(exc) from exc

        # Pumped storage reports both generation and consumption, so the second
        # level has to survive or the two collapse into duplicate rows.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [
                f"{name} Consumption" if measure == "Actual Consumption" else name
                for name, measure in raw.columns
            ]

        raw.index = _to_utc(raw.index)
        long = (
            raw.rename_axis("ts_utc")
            .reset_index()
            .melt(id_vars=["ts_utc"], var_name="production_type", value_name="generation_mw")
        )
        return _with_resolution(long)


class EntsoePriceSource(Source):
    name = "entsoe_day_ahead_price"
    schema = PRICE_SCHEMA

    def fetch(self, zone_code: str, start: datetime, end: datetime) -> pd.DataFrame:
        try:
            raw = _client().query_day_ahead_prices(
                ZONE_TO_AREA[zone_code], start=pd.Timestamp(start), end=pd.Timestamp(end)
            )
        except Exception as exc:
            raise _classify(exc) from exc

        frame = raw.rename("price_eur_mwh").to_frame()
        frame.index = _to_utc(frame.index)
        return _with_resolution(frame.rename_axis("ts_utc").reset_index())