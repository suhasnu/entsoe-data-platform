{{ config(severity='warn') }}

-- An hour should hold exactly 60 / resolution intervals. Fewer means the source
-- published a partial hour, and nothing in the row itself gives that away.
-- Warn rather than error: the gaps are upstream and cannot be fixed by failing
-- the build, but they should be visible on every run.

select
    zone_code,
    ts_hour_utc,
    source_resolution_minutes,
    intervals_aggregated,
    div(60, source_resolution_minutes) as intervals_expected
from {{ ref('stg_load') }}
where intervals_aggregated != div(60, source_resolution_minutes)