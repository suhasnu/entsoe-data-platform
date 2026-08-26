{{ config(materialized='table') }}

select
    hour_utc,
    case
        when hour_utc between 6 and 9 then 'Morning ramp'
        when hour_utc between 10 and 15 then 'Midday'
        when hour_utc between 16 and 20 then 'Evening peak'
        else 'Overnight'
    end as time_band,
    -- European power markets trade 08:00 to 20:00 CET as peak.
    hour_utc between 6 and 17 as is_peak_hour
from unnest(generate_array(0, 23)) as hour_utc