# entsoe-data-platform

Hourly and quarter-hourly electricity load, generation mix, and day-ahead prices
for six European bidding zones, ingested from the ENTSO-E Transparency Platform
into a partitioned BigQuery warehouse.

**Status:** in development. Ingestion and bronze layer working; dbt models,
orchestration, and serving layer to follow.

## Stack

Python 3.11 · BigQuery · dbt · Airflow · Docker · pandera · pytest

## What it does today

- Pulls actual load, generation by production type, and day-ahead prices for
  DE_LU, AT, NL, FR, DK_1 and DK_2
- Validates every frame against a pandera contract before it reaches the warehouse
- Writes to an append-only bronze layer, partitioned by month and clustered by zone

## Design notes

**Source resolution varies by zone.** DE_LU, AT, NL and FR publish at 15 minute
resolution; DK_1 and DK_2 at 60. Bronze stores the native grain with a
`resolution_minutes` column rather than degrading at ingestion, so finer analysis
stays possible without re-ingesting. Conforming to a common hourly grain happens
in the staging layer.

**Market days are not always 24 hours.** A trading day runs local midnight to
local midnight in Europe/Berlin, which is 23 hours on the spring clock change and
25 on the autumn one. Windows are built timezone-aware and returned in UTC;
storage is UTC throughout.

**Bronze is append-only.** Re-running a day writes a new version with a fresh
`ingested_at` rather than overwriting, so replays are safe and upstream revisions
are preserved. Deduplication happens downstream in dbt.

**Partition filters are required.** BigQuery bills on bytes scanned, so bronze
tables set `require_partition_filter`, and a query without a date filter is
rejected rather than run expensively.

## Local setup

```bash
git clone https://github.com/suhasnu/entsoe-data-platform.git
cd entsoe-data-platform
py -3.11 -m venv .venv && .venv\Scripts\Activate.ps1
pip install -e ".[dev]"
cp .env.example .env        # fill in ENTSOE_API_KEY and GCP_PROJECT_ID
python -m gridflow.storage.migrate
pytest
```

Requires an ENTSO-E API key (request from transparency@entsoe.eu) and a Google
Cloud service account with BigQuery Data Editor and Job User roles.

## Data licensing

Electricity data © ENTSO-E Transparency Platform, used under its terms of use.
No personal data is processed.