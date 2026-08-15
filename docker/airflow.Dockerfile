FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY --chown=airflow:root pyproject.toml README.md /tmp/build/
COPY --chown=airflow:root src /tmp/build/src

RUN pip install --no-cache-dir \
      "dbt-core>=1.8,<2" \
      "dbt-bigquery>=1.8,<2" \
      "apache-airflow-providers-google" \
 && pip install --no-cache-dir /tmp/build