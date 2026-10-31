{{ config(severity='warn') }}

-- A whole hour can be missing without any partial hour existing, so counting
-- intervals is not enough. A market day runs local midnight to local midnight,
-- which is 23 hours on the spring clock change and 25 on the autumn one, so the
-- expected count is derived rather than hard coded.

with per_day as (

    select
        zone_code,
        date(ts_hour_utc, 'Europe/Berlin') as market_day,
        count(*) as hours_loaded
    from {{ ref('stg_load') }}
    group by zone_code, market_day

),

expected as (

    select
        *,
        timestamp_diff(
            timestamp(datetime(date_add(market_day, interval 1 day)), 'Europe/Berlin'),
            timestamp(datetime(market_day), 'Europe/Berlin'),
            hour
        ) as hours_expected
    from per_day

)

select *
from expected
-- Ingestion runs on a two day lag, so anything newer is legitimately partial.
where market_day < date_sub(current_date('Europe/Berlin'), interval 3 day)
  and hours_loaded != hours_expected