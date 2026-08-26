{{ config(materialized='table') }}

with date_spine as (
    select date_day
    from unnest(generate_date_array('2023-01-01', '2028-12-31')) as date_day
)

select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month_number,
    format_date('%B', date_day) as month_name,
    format_date('%Y-%m', date_day) as year_month,
    extract(isoweek from date_day) as iso_week,
    extract(dayofweek from date_day) as day_of_week_number,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    date_trunc(date_day, month) as month_start,
    date_trunc(date_day, quarter) as quarter_start
from date_spine