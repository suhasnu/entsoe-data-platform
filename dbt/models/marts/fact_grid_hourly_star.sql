{{ config(
    materialized='table',
    partition_by={'field': 'date_key', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['zone_code']
) }}

select
    zone_code,
    date_local as date_key,
    hour_utc,
    ts_hour_utc,

    load_mw,
    renewable_mw,
    fossil_mw,
    nuclear_mw,
    storage_mw,
    other_mw,
    total_generation_mw,
    storage_consumption_mw,
    residual_load_mw,
    price_eur_mwh
from {{ ref('fct_grid_hourly') }}