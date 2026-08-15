select zone_code, ts_hour_utc, renewable_pct
from {{ ref('fct_grid_hourly') }}
where renewable_pct < 0 or renewable_pct > 100