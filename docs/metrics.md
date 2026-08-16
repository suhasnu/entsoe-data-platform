# Measured results

Backfill, 13 May to 13 August 2026:
- 1,799 Airflow task instances, 0 failures
- 91 days across 6 bidding zones
- 13,096 rows in fct_grid_hourly
- Mean DAG run duration 1m 22s, max 2m 44s
- dbt build: 20 nodes, 14 tests, 35 seconds

Warehouse:
- 41.8 MiB scanned to build the fact table