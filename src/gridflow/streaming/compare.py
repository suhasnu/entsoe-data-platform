from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

from gridflow.config import get_settings

BATCH_COLUMNS = ["zone_code", "ts_hour_utc", "load_mw_batch", "intervals_batch"]
STREAM_COLUMNS = ["zone_code", "ts_hour_utc", "load_mw_stream", "intervals_stream"]


def read_stream_hourly(root: Path) -> pd.DataFrame:
    frame = pd.read_parquet(root)
    frame["zone_code"] = frame["zone_code"].astype(str)
    frame["ts_hour_utc"] = pd.to_datetime(frame["ts_hour_utc"], utc=True)
    frame = frame.rename(columns={"load_mw": "load_mw_stream", "intervals": "intervals_stream"})
    return frame[STREAM_COLUMNS].sort_values(["zone_code", "ts_hour_utc"]).reset_index(drop=True)


def read_batch_hourly(start: datetime, end: datetime, zones: list[str]) -> pd.DataFrame:
    settings = get_settings()
    client = bigquery.Client(project=settings.gcp_project_id)
    query = f"""
        select
            zone_code,
            ts_hour_utc,
            load_mw as load_mw_batch,
            intervals_aggregated as intervals_batch
        from `{settings.gcp_project_id}.{settings.bq_dataset_staging}.stg_load`
        where ts_hour_utc >= @start
          and ts_hour_utc <  @end
          and zone_code in unnest(@zones)
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
                bigquery.ArrayQueryParameter("zones", "STRING", zones),
            ],
            maximum_bytes_billed=200_000_000,
        ),
    )
    frame = job.to_dataframe()
    frame["zone_code"] = frame["zone_code"].astype(str)
    frame["ts_hour_utc"] = pd.to_datetime(frame["ts_hour_utc"], utc=True)
    return frame[BATCH_COLUMNS].sort_values(["zone_code", "ts_hour_utc"]).reset_index(drop=True)


def compare(
    batch: pd.DataFrame, stream: pd.DataFrame, tolerance: float = 0.5
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Full outer join on the natural key, so neither path hides the other's gaps."""
    merged = batch.merge(
        stream, on=["zone_code", "ts_hour_utc"], how="outer", indicator=True
    ).sort_values(["zone_code", "ts_hour_utc"])
    merged["delta_mw"] = merged["load_mw_batch"] - merged["load_mw_stream"]

    both = merged[merged["_merge"] == "both"]
    summary = {
        "hours_batch": int((merged["_merge"] != "right_only").sum()),
        "hours_stream": int((merged["_merge"] != "left_only").sum()),
        "hours_in_both": len(both),
        "batch_only": int((merged["_merge"] == "left_only").sum()),
        "stream_only": int((merged["_merge"] == "right_only").sum()),
        "load_disagreements": int((both["delta_mw"].abs() > tolerance).sum()),
        "max_abs_delta_mw": float(both["delta_mw"].abs().max()) if len(both) else 0.0,
        "interval_disagreements": int((both["intervals_batch"] != both["intervals_stream"]).sum()),
    }
    return merged.reset_index(drop=True), summary
