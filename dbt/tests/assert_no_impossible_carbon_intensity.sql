-- Lignite is the dirtiest thing on the grid at about 1050 g/kWh, so any mix
-- averaging above that means the seed or the weighting is wrong.
select zone_code, ts_hour_utc, carbon_intensity_g_kwh
from {{ ref('fct_grid_hourly') }}
where carbon_intensity_g_kwh < 0 or carbon_intensity_g_kwh > 1100