# entsoe-data-platform

Hourly electricity load, generation mix, and day-ahead prices for European
bidding zones, ingested from the ENTSO-E Transparency Platform, modelled
into a tested warehouse, and served via an API.

**Status:** in development.

## Stack

Python 3.11 · PostgreSQL + TimescaleDB · Airflow · dbt · FastAPI · Docker

## Data licensing

Electricity data © ENTSO-E Transparency Platform, used under its terms of use.
Weather data from Open-Meteo (CC BY 4.0). No personal data is processed.