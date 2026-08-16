from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

BRONZE = Dataset("gridflow://bronze")
DBT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="transform_dbt",
    schedule=[BRONZE],
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "suhas",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["dbt", "staging", "marts"],
)
def transform_dbt():
    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir .",
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir . --fail-fast",
    )

    freshness >> build


transform_dbt()
