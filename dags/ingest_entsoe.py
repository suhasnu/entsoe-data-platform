from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from gridflow.cli import SOURCES
from gridflow.ingestion.base import PermanentSourceError
from gridflow.storage.writers import load_to_bronze


@dag(
    dag_id="ingest_entsoe",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "suhas",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "entsoe", "bronze"],
)
def ingest_entsoe():
    @task
    def build_jobs(**context) -> list[dict[str, str]]:
        target = (context["logical_date"] - timedelta(days=2)).date().isoformat()
        from gridflow.config import get_settings

        return [
            {"zone": zone, "source": source, "day": target}
            for source in SOURCES
            for zone in get_settings().zones
        ]

    @task(
        map_index_template=(
            "{{ task.op_kwargs['job']['source'] }}-{{ task.op_kwargs['job']['zone'] }}"
        )
    )
    def ingest_one(job: dict[str, str]) -> dict[str, str]:
        source_cls, table = SOURCES[job["source"]]
        try:
            result = source_cls().ingest(job["zone"], date.fromisoformat(job["day"]))
            load_to_bronze(result.frame, table)
        except PermanentSourceError as exc:
            # Retrying will not conjure data that does not exist upstream.
            raise AirflowFailException(str(exc)) from exc
        return job

    ingest_one.expand(job=build_jobs())


ingest_entsoe()
