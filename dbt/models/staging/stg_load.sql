{{ config(materialized='view') }}

with latest as (

    select
        zone_code,
        ts_utc,
        load_mw,
        resolution_minutes,
        ingested_at,
        row_number() over (
            partition by zone_code, ts_utc
            order by ingested_at desc
        ) as revision_rank
    from {{ source('bronze', 'raw_load') }}
    where ts_utc >= timestamp('2020-01-01')

)

select
    zone_code,
    timestamp_trunc(ts_utc, hour) as ts_hour_utc,
    avg(load_mw) as load_mw,
    count(*) as intervals_aggregated,
    max(resolution_minutes) as source_resolution_minutes,
    max(ingested_at) as ingested_at
from latest
where revision_rank = 1
  and load_mw is not null
group by zone_code, ts_hour_utc