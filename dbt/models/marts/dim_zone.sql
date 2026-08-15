{{ config(materialized='table') }}

select * from unnest([
    struct('DE_LU' as zone_code, 'Germany/Luxembourg' as zone_name, 'DE' as country, 'Europe/Berlin' as timezone),
    struct('AT', 'Austria', 'AT', 'Europe/Vienna'),
    struct('NL', 'Netherlands', 'NL', 'Europe/Amsterdam'),
    struct('FR', 'France', 'FR', 'Europe/Paris'),
    struct('DK_1', 'Denmark West', 'DK', 'Europe/Copenhagen'),
    struct('DK_2', 'Denmark East', 'DK', 'Europe/Copenhagen')
])