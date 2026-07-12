# Weather Data Engineering Pipeline

An end-to-end data engineering project that extracts current weather data from
the **OpenWeather API**, loads it into **Snowflake**, and produces aggregated
analytics — all orchestrated with **Apache Airflow** running on the
**Astronomer (Astro)** runtime.

---

## Table of Contents

- [Overview](#overview)
- [Architecture & Workflow](#architecture--workflow)
- [Project Structure](#project-structure)
- [The Two Pipelines (DAGs)](#the-two-pipelines-dags)
- [Snowflake Data Model](#snowflake-data-model)
- [Configuration](#configuration)
- [Running Locally](#running-locally)
- [Module Reference](#module-reference)

---

## Overview

The project implements a classic **ELT** (Extract → Load → Transform) flow split
across two scheduled pipelines:

| Pipeline | DAG ID | Schedule | Purpose |
|----------|--------|----------|---------|
| Hourly ingestion | `weather_hourly_ingestion` | Every hour (`0 * * * *`) | Pull weather data and land it in staging |
| Daily analytics | `weather_daily_analytics` | Every midnight (`0 0 * * *`) | Validate, aggregate, and split by city |

Weather is collected for a fixed list of cities:
**Colombo, Dubai, New York, London, Sydney**.

---

## Architecture & Workflow

```
                        ┌─────────────────────────────────────────────┐
                        │              OpenWeather API                 │
                        └──────────────────────┬──────────────────────┘
                                               │ (1) fetch current weather
                                               ▼
   HOURLY DAG          ┌─────────────────────────────────────────────┐
 weather_hourly_       │  extract_weather_api                         │
 ingestion             │  weather_extractor.run_extraction_pipeline   │
                       │  → saves raw JSON to data/raw/*.json         │
                       └──────────────────────┬──────────────────────┘
                                               │ (2)
                                               ▼
                       ┌─────────────────────────────────────────────┐
                       │  create_schemas                              │
                       │  snowflake.db.schema.create_schemas          │
                       │  → CREATE DB / SCHEMA / TABLES IF NOT EXISTS │
                       └──────────────────────┬──────────────────────┘
                                               │ (3)
                                               ▼
                       ┌─────────────────────────────────────────────┐
                       │  load_weather_staging                        │
                       │  snowflake.data_loader.snowflake_loader.main │
                       │  → INSERT rows into WEATHER_STAGING          │
                       └──────────────────────┬──────────────────────┘
                                               │
                                               ▼
                              ╔══════════════════════════════╗
                              ║   Snowflake: WEATHER_STAGING  ║
                              ╚═══════════════┬══════════════╝
                                               │
   DAILY DAG                                   │ (4)
 weather_daily_                                ▼
 analytics             ┌─────────────────────────────────────────────┐
                       │  data_quality_check                          │
                       │  data_analysis.validate_data                 │
                       │  → fail if NULLs or duplicates found          │
                       └──────────────────────┬──────────────────────┘
                                               │ (5)
                                               ▼
                       ┌─────────────────────────────────────────────┐
                       │  aggregate_weather_data                      │
                       │  aggregate_splitting.analyze_weather_data    │
                       │  → INSERT aggregates into WEATHER_ANALYTICS  │
                       └──────────────────────┬──────────────────────┘
                                               │ (6)
                                               ▼
                       ┌─────────────────────────────────────────────┐
                       │  split_city_tables                           │
                       │  aggregate_splitting.split_city_tables       │
                       │  → CREATE one table per city                 │
                       └─────────────────────────────────────────────┘
```

---

## Project Structure

```
Data-Eng-Session/
├── dags/                              # Airflow DAG definitions
│   ├── hourly_data_ingestion.py       # weather_hourly_ingestion DAG
│   └── daily_analytical_pipeline.py   # weather_daily_analytics DAG
│
├── weather_extractor/
│   └── weather_extractor.py           # OpenWeather API client + extraction
│
├── snowflake/
│   ├── db/
│   │   └── schema.py                  # Creates DB / schema / tables
│   ├── data_loader/
│   │   └── snowflake_loader.py        # Loads raw JSON → WEATHER_STAGING
│   └── data_analysis/
│       ├── data_analysis.py           # Data quality checks
│       └── aggregate_splitting.py     # Aggregation + per-city split
│
├── config/
│   └── config.py                      # OpenWeather & Snowflake config classes
│
├── data/raw/                          # Raw JSON files land here
├── tests/                             # DAG integrity tests
├── Dockerfile                         # Astro Runtime image
├── requirements.txt                   # Python dependencies
├── airflow_settings.yaml              # Local Airflow connections/vars/pools
├── .env.sample                        # Template for required secrets
└── README.md
```

---

## The Two Pipelines (DAGs)

### 1. Hourly Ingestion — `weather_hourly_ingestion`

Runs every hour and executes three tasks in sequence:

```
extract_weather_api → create_schemas → load_weather_staging
```

1. **`extract_weather_api`** — Calls the OpenWeather API for each configured
   city, stamps each record with an `_extracted_at` UTC timestamp, and writes
   all records to a timestamped file in `data/raw/`.
2. **`create_schemas`** — Idempotently creates the `WEATHER_DB` database, the
   `RAW` schema, and the `WEATHER_STAGING` / `WEATHER_ANALYTICS` tables.
3. **`load_weather_staging`** — Reads the raw JSON files, flattens each nested
   OpenWeather record into a row, and bulk-inserts them into `WEATHER_STAGING`.

### 2. Daily Analytics — `weather_daily_analytics`

Runs at midnight and executes three tasks in sequence:

```
data_quality_check → aggregate_weather_data → split_city_tables
```

1. **`data_quality_check`** — Verifies `WEATHER_STAGING` has no NULLs in
   required columns and no duplicate `(city, observation_time)` rows. Raises an
   error to **stop the pipeline** if data quality fails.
2. **`aggregate_weather_data`** — Computes per-`(city, country)` averages
   (temperature, humidity, wind speed) plus min/max temperature and inserts the
   results into `WEATHER_ANALYTICS`.
3. **`split_city_tables`** — Creates one table per city (e.g. `COLOMBO`,
   `NEW_YORK`) from the analytics table for convenient per-city querying.

---

## Snowflake Data Model

**Database:** `WEATHER_DB`  **Schema:** `RAW`

### `WEATHER_STAGING` — raw ingested rows

| Column | Type | Description |
|--------|------|-------------|
| `city` | STRING | City name |
| `country` | STRING | Country code |
| `temperature` | FLOAT | Current temperature (°C) |
| `humidity` | FLOAT | Humidity (%) |
| `wind_speed` | FLOAT | Wind speed |
| `weather_condition` | STRING | Text weather description |
| `observation_time` | TIMESTAMP | Extraction timestamp (`_extracted_at`) |

### `WEATHER_ANALYTICS` — aggregated results

| Column | Type | Description |
|--------|------|-------------|
| `city` | STRING | City name |
| `country` | STRING | Country code |
| `avg_temperature` | FLOAT | Average temperature |
| `avg_humidity` | FLOAT | Average humidity |
| `avg_wind_speed` | FLOAT | Average wind speed |
| `max_temperature` | FLOAT | Maximum temperature |
| `min_temperature` | FLOAT | Minimum temperature |
| `created_at` | TIMESTAMP | Row creation time (defaults to now) |

### Per-city tables

`split_city_tables` creates one table per distinct city (name uppercased, spaces
→ underscores) containing that city's analytics rows.

---

## Configuration

All configuration lives in [config/config.py](config/config.py) and is driven by
environment variables. Copy [.env.sample](.env.sample) to `.env` and fill in the
values:

```dotenv
## Open Weather
WEAHTER_API_KEY=""      # your OpenWeather API key
BASE_URL=""             # defaults to the OpenWeather current-weather endpoint

## Snowflake
SNOWFLAKE_ACCOUNT=""
SNOWFLAKE_USER=""
SNOWFLAKE_PASSWORD=""
SNOWFLAKE_ROLE=""
SNOWFLAKE_WAREHOUSE=""
```

> **Note:** the API key variable is spelled `WEAHTER_API_KEY` in the code — keep
> it consistent in your `.env` file.

Two config classes are exposed:

- **`OpenWeatherConfig`** — `api_key`, `base_url`, `units` (`metric` = °C),
  `time_out` (request timeout in seconds).
- **`SnowflakeConfig`** — connection credentials plus the database, schema, and
  table names (`WEATHER_DB`, `RAW`, `WEATHER_STAGING`, `WEATHER_ANALYTICS`).

---

## Running Locally

This is an [Astronomer](https://www.astronomer.io/) project. With the
[Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) and Docker
installed:

```bash
# 1. Create your .env from the sample and fill in secrets
cp .env.sample .env

# 2. Start Airflow locally (spins up scheduler, API server, Postgres, etc.)
astro dev start
```

The Airflow UI opens at **http://localhost:8080/**. From there you can trigger
or monitor the `weather_hourly_ingestion` and `weather_daily_analytics` DAGs.

To stop the environment:

```bash
astro dev stop
```

### Python dependencies

Defined in [requirements.txt](requirements.txt):

- `python-dotenv` — load environment variables from `.env`
- `loguru` — structured logging
- `requests` — HTTP client for the OpenWeather API
- `apache-airflow` — orchestration
- `snowflake-connector-python` — Snowflake connectivity

---

## Module Reference

Every Python module and public function is documented with docstrings. Quick map:

| Module | Key functions | Role |
|--------|---------------|------|
| [weather_extractor/weather_extractor.py](weather_extractor/weather_extractor.py) | `WeatherExtractor`, `run_extraction_pipeline` | Fetch weather from API, save raw JSON |
| [config/config.py](config/config.py) | `OpenWeatherConfig`, `SnowflakeConfig` | Central configuration |
| [snowflake/db/schema.py](snowflake/db/schema.py) | `create_schemas` | Create DB/schema/tables |
| [snowflake/data_loader/snowflake_loader.py](snowflake/data_loader/snowflake_loader.py) | `load_raw_json_files`, `insert_weather_data`, `main` | Load JSON → staging |
| [snowflake/data_analysis/data_analysis.py](snowflake/data_analysis/data_analysis.py) | `is_null`, `is_duplicate`, `validate_data` | Data quality gate |
| [snowflake/data_analysis/aggregate_splitting.py](snowflake/data_analysis/aggregate_splitting.py) | `analyze_weather_data`, `split_city_tables` | Aggregate + split by city |

---

*Orchestrated with Apache Airflow on the Astronomer runtime. Data warehoused in Snowflake.*

## astro install command
winget install astronomer.astro