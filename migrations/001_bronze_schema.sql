create extension if not exists timescaledb;

create schema if not exists bronze;

create table if not exists bronze.raw_load (
    zone_code   text        not null,
    ts_utc      timestamptz not null,
    load_mw     double precision,
    ingested_at timestamptz not null default now(),
    run_id      uuid        not null,
    primary key (zone_code, ts_utc, ingested_at)
);

create table if not exists bronze.raw_generation (
    zone_code       text        not null,
    ts_utc          timestamptz not null,
    production_type text        not null,
    generation_mw   double precision,
    ingested_at     timestamptz not null default now(),
    run_id          uuid        not null,
    primary key (zone_code, ts_utc, production_type, ingested_at)
);

create table if not exists bronze.raw_day_ahead_price (
    zone_code     text        not null,
    ts_utc        timestamptz not null,
    price_eur_mwh double precision,
    ingested_at   timestamptz not null default now(),
    run_id        uuid        not null,
    primary key (zone_code, ts_utc, ingested_at)
);

-- Hypertables partition by time automatically. One month chunks suit an hourly
-- feed: small enough to prune well, large enough to avoid chunk sprawl.
select create_hypertable('bronze.raw_load', 'ts_utc',
                         chunk_time_interval => interval '1 month',
                         if_not_exists => true);

select create_hypertable('bronze.raw_generation', 'ts_utc',
                         chunk_time_interval => interval '1 month',
                         if_not_exists => true);

create index if not exists idx_raw_load_zone_ts
    on bronze.raw_load (zone_code, ts_utc desc);

create index if not exists idx_raw_generation_zone_ts
    on bronze.raw_generation (zone_code, ts_utc desc);

create index if not exists idx_raw_price_zone_ts
    on bronze.raw_day_ahead_price (zone_code, ts_utc desc);