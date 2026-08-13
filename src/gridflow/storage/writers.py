import pandas as pd
from google.cloud import bigquery

from gridflow.config import get_settings
from gridflow.logging import get_logger

log = get_logger(__name__)

BQ_FIELDS = {
    "zone_code": ("STRING", "REQUIRED"),
    "ts_utc": ("TIMESTAMP", "REQUIRED"),
    "production_type": ("STRING", "REQUIRED"),
    "load_mw": ("FLOAT64", "NULLABLE"),
    "generation_mw": ("FLOAT64", "NULLABLE"),
    "price_eur_mwh": ("FLOAT64", "NULLABLE"),
    "resolution_minutes": ("INT64", "REQUIRED"),
    "ingested_at": ("TIMESTAMP", "REQUIRED"),
    "run_id": ("STRING", "REQUIRED"),
}


def _client() -> bigquery.Client:
    settings = get_settings()
    return bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)


def load_to_bronze(frame: pd.DataFrame, table: str) -> int:
    """Append a validated frame to a bronze table.

    Bronze is append only, so a replay writes a new version rather than
    overwriting. Deduplication happens in the dbt staging layer.
    """
    if frame.empty:
        log.warning("load.empty", table=table)
        return 0

    settings = get_settings()
    frame = frame.assign(ingested_at=pd.Timestamp.now(tz="UTC"))
    table_id = f"{settings.gcp_project_id}.{settings.bq_dataset_bronze}.{table}"

    # Explicit schema rather than autodetect: a batch where every price happens
    # to be a whole number would otherwise be typed INT64 and break the next load.
    # Modes have to match the table exactly: BigQuery defaults to NULLABLE and
    # rejects the load if the table declares a column REQUIRED.
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField(name, field_type, mode=mode)
            for name, (field_type, mode) in BQ_FIELDS.items()
            if name in frame.columns
        ],
    )

    _client().load_table_from_dataframe(frame, table_id, job_config=job_config).result()
    log.info("load.done", table=table, rows=len(frame))
    return len(frame)