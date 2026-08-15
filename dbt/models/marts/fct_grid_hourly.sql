{{ config(
    materialized='table',
    partition_by={'field': 'ts_hour_utc', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['zone_code']
) }}

with generation_mix as (

    select
        g.zone_code,
        g.ts_hour_utc,
        sum(case when d.category = 'renewable' then g.generation_mw else 0 end) as renewable_mw,
        sum(case when d.category = 'fossil'    then g.generation_mw else 0 end) as fossil_mw,
        sum(case when d.category = 'nuclear'   then g.generation_mw else 0 end) as nuclear_mw,
        sum(case when d.category = 'storage'   then g.generation_mw else 0 end) as storage_mw,
        sum(case when d.category = 'other'     then g.generation_mw else 0 end) as other_mw,
        sum(g.generation_mw) as total_generation_mw,
        sum(g.generation_mw * d.emission_factor_kg_co2_mwh) as total_co2_kg
    from {{ ref('stg_generation') }} as g
    inner join {{ ref('dim_production_type') }} as d using (production_type)
    where d.flow_direction = 'generation'
    group by g.zone_code, g.ts_hour_utc

),

storage_draw as (

    select
        g.zone_code,
        g.ts_hour_utc,
        sum(g.generation_mw) as storage_consumption_mw
    from {{ ref('stg_generation') }} as g
    inner join {{ ref('dim_production_type') }} as d using (production_type)
    where d.flow_direction = 'consumption'
      and d.category = 'storage'
    group by g.zone_code, g.ts_hour_utc

)

select
    l.zone_code,
    l.ts_hour_utc,
    date(l.ts_hour_utc, z.timezone) as date_local,

    l.load_mw,
    m.renewable_mw,
    m.fossil_mw,
    m.nuclear_mw,
    m.storage_mw,
    m.other_mw,
    m.total_generation_mw,
    s.storage_consumption_mw,

    case
        when m.total_generation_mw > 0
        then round(m.renewable_mw / m.total_generation_mw * 100, 2)
    end as renewable_pct,

    -- What dispatchable plant and imports have to cover once wind and solar are in.
    l.load_mw - coalesce(m.renewable_mw, 0) as residual_load_mw,

    case
        when m.total_generation_mw > 0
        then round(m.total_co2_kg / m.total_generation_mw, 1)
    end as carbon_intensity_g_kwh,

    p.price_eur_mwh,

    extract(hour from l.ts_hour_utc) as hour_utc,
    extract(dayofweek from datetime(l.ts_hour_utc, z.timezone)) as day_of_week_local,
    l.intervals_aggregated as load_intervals_aggregated

from {{ ref('stg_load') }} as l
inner join {{ ref('dim_zone') }} as z using (zone_code)
left join generation_mix as m using (zone_code, ts_hour_utc)
left join storage_draw as s using (zone_code, ts_hour_utc)
left join {{ ref('stg_day_ahead_price') }} as p using (zone_code, ts_hour_utc)