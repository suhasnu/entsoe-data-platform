{{ config(materialized='view') }}

with latest as (

    select
        zone_code,
        ts_utc,
        production_type,
        generation_mw,
        resolution_minutes,
        ingested_at,
        row_number() over (
            partition by zone_code, ts_utc, production_type
            order by ingested_at desc
        ) as revision_rank
    from {{ source('bronze', 'raw_generation') }}
    where ts_utc >= timestamp('2020-01-01')

)

select
    zone_code,
    timestamp_trunc(ts_utc, hour) as ts_hour_utc,
    production_type,
    avg(generation_mw) as generation_mw,
    count(*) as intervals_aggregated,
    max(resolution_minutes) as source_resolution_minutes,
    max(ingested_at) as ingested_at
from latest
where revision_rank = 1
  and generation_mw is not null
group by zone_code, ts_hour_utc, production_type