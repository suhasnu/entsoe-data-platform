import base64
from datetime import UTC, datetime, timedelta

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from google.cloud import bigquery

from gridflow.api.schemas import GridHour, Page, Zone
from gridflow.config import get_settings
from gridflow.logging import configure_logging, get_logger

settings = get_settings()
configure_logging(settings.log_level, json_output=settings.env != "local")
log = get_logger(__name__)

app = FastAPI(
    title="ENTSO-E Data Platform API",
    version="0.1.0",
    description="Hourly European electricity load, generation mix and day-ahead prices.",
)


def client() -> bigquery.Client:
    return bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)


async def require_api_key(x_api_key: str = Header(...)) -> str:
    if x_api_key not in settings.api_keys:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing API key")
    return x_api_key


def encode_cursor(ts: datetime) -> str:
    return base64.urlsafe_b64encode(ts.isoformat().encode()).decode()


def decode_cursor(cursor: str | None) -> datetime | None:
    if not cursor:
        return None
    try:
        return datetime.fromisoformat(base64.urlsafe_b64decode(cursor).decode())
    except Exception as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "malformed cursor") from exc


@app.get("/health", tags=["ops"])
async def health() -> dict[str, str]:
    return {"status": "ok", "version": app.version}


@app.get("/v1/zones", response_model=list[Zone], tags=["reference"])
async def zones(_: str = Depends(require_api_key)) -> list[Zone]:
    sql = f"""
        select zone_code, zone_name, country, timezone
        from `{settings.gcp_project_id}.{settings.bq_dataset_marts}.dim_zone`
        order by zone_code
    """
    return [Zone(**dict(row)) for row in client().query(sql).result()]


@app.get("/v1/grid/hourly", response_model=Page[GridHour], tags=["grid"])
async def grid_hourly(
    zone: str = Query(..., pattern=r"^[A-Z]{2}(_[A-Z0-9]+)?$"),
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(168, ge=1, le=1000),
    cursor: str | None = None,
    _: str = Depends(require_api_key),
) -> Page[GridHour]:
    end = end or datetime.now(UTC)
    start = start or end - timedelta(days=7)
    if start >= end:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "start must be before end")

    after = decode_cursor(cursor)

    sql = f"""
        select zone_code, ts_hour_utc, date_local, load_mw, renewable_mw, fossil_mw,
               nuclear_mw, total_generation_mw, renewable_pct, residual_load_mw,
               carbon_intensity_g_kwh, price_eur_mwh
        from `{settings.gcp_project_id}.{settings.bq_dataset_marts}.fct_grid_hourly`
        where zone_code = @zone
          and ts_hour_utc >= @start
          and ts_hour_utc < @end
          and (@after is null or ts_hour_utc > @after)
        order by ts_hour_utc
        limit @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("zone", "STRING", zone),
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            bigquery.ScalarQueryParameter("after", "TIMESTAMP", after),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
        maximum_bytes_billed=100_000_000,
    )

    rows = [GridHour(**dict(r)) for r in client().query(sql, job_config=job_config).result()]
    next_cursor = encode_cursor(rows[-1].ts_hour_utc) if len(rows) == limit else None

    log.info("api.grid_hourly", zone=zone, rows=len(rows))
    return Page[GridHour](items=rows, count=len(rows), next_cursor=next_cursor)
