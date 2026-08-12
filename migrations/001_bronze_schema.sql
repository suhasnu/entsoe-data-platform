create schema if not exists `{project}.bronze`
options (location = 'EU');

create table if not exists `{project}.bronze.raw_load` (
    zone_code   string      not null,
    ts_utc      timestamp   not null,
    resolution_minutes int64 not null,
    load_mw     float64,
    ingested_at timestamp   not null,
    run_id      string      not null
)
partition by timestamp_trunc(ts_utc, month)
cluster by zone_code
options (require_partition_filter = true);

create table if not exists `{project}.bronze.raw_generation` (
    zone_code       string    not null,
    ts_utc          timestamp not null,
    resolution_minutes int64 not null,
    production_type string    not null,
    generation_mw   float64,
    ingested_at     timestamp not null,
    run_id          string    not null
)
partition by timestamp_trunc(ts_utc, month)
cluster by zone_code, production_type
options (require_partition_filter = true);

create table if not exists `{project}.bronze.raw_day_ahead_price` (
    zone_code     string    not null,
    ts_utc        timestamp not null,
    resolution_minutes int64 not null,
    price_eur_mwh float64,
    ingested_at   timestamp not null,
    run_id        string    not null
)
partition by timestamp_trunc(ts_utc, month)
cluster by zone_code
options (require_partition_filter = true);