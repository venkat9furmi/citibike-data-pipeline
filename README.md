# Citi Bike NYC — Supply Gap Pipeline

A production-grade data engineering pipeline on Google Cloud Platform that answers:

> **Which Citi Bike stations have the highest historical trip demand but lowest real-time bike availability — and when does this supply gap peak by hour of day?**

## Architecture

```
Citi Bike S3 (trip CSVs)          GBFS Live API (real-time)
        |                                   |
        | fetch_trip_history.py             | fetch_realtime_gbfs.py
        v                                   v
  GCS raw/trip_history/            GCS raw/gbfs/station_status/
        |
        | Dataproc Serverless (Spark)
        v
  GCS processed/trips/ (Parquet)
        |
        | BigQuery External Tables
        v
  citibike_raw dataset
  (trips_parquet + station_status_json + station_information)
        |
        | dbt transformations
        v
  citibike_staging (stg_* and int_* views)
        |
        v
  citibike_marts.mart_supply_gap_analysis  <- FINAL ANSWER (294,912 rows)
```

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | Data ingestion (trip history + GBFS live feed) |
| Google Cloud Storage | Raw and processed data lake |
| Dataproc Serverless | Managed Spark for large-scale trip processing |
| BigQuery | Data warehouse (external + native tables) |
| dbt Core | SQL transformations and data modeling |
| Cloud Functions | Serverless wrappers for automation |
| Cloud Scheduler | Cron-based pipeline scheduling |
| Looker Studio | Interactive dashboard visualization |

## GCP Project

- **Project ID:** citibike-pipeline-499418
- **Region:** us-east1

## Key Results

- **4,681,211** historical trips processed (May 2026)
- **2,411** stations tracked with real-time availability
- **294,912** rows in the final supply gap mart
- **Worst gap:** 9 Ave & W 33 St at 5pm — 653 departures, only 25% bikes available

## Repository Structure

```
citibike-pipeline/
├── config/config.yaml              # Central configuration
├── ingestion/
│   ├── fetch_trip_history.py       # Download monthly trip CSVs from S3
│   └── fetch_realtime_gbfs.py      # Fetch live GBFS station status
├── spark/process_trips.py          # PySpark: clean + transform trip data
├── bigquery/
│   ├── create_external_tables.py   # Create BQ external tables over GCS
│   └── load_station_info.py        # Load GBFS station info into BQ
├── dbt/
│   ├── models/staging/             # stg_trip_history, stg_station_status
│   ├── models/intermediate/        # int_station_hourly_demand, int_station_avg_availability
│   └── models/marts/               # mart_supply_gap_analysis (final table)
├── cloud_functions/
│   ├── gbfs_ingest/                # Cloud Function: GBFS ingestion
│   └── dbt_run/                    # Cloud Function: dbt execution
└── deploy_scheduler.ps1            # Deploy Cloud Functions + Scheduler jobs
```

## Automated Schedule

| Job | Schedule | Action |
|---|---|---|
| `gbfs-every-15min` | Every 15 minutes | Fetch live GBFS station status |
| `spark-daily` | 2:00am daily | Run Spark to process new trip data |
| `dbt-daily` | 4:00am daily | Refresh all dbt models and mart |

## Setup

### Prerequisites
- Python 3.14 (ingestion) and Python 3.12 (dbt — incompatible with 3.14)
- Google Cloud SDK (`gcloud`)
- GCP project with BigQuery, Dataproc, Cloud Functions APIs enabled
- Run `gcloud auth application-default login` before executing scripts

### Run ingestion manually
```bash
venv\Scripts\activate
python ingestion/fetch_trip_history.py
python ingestion/fetch_realtime_gbfs.py
```

### Submit Spark job
```bash
gcloud dataproc batches submit pyspark gs://citibike-pipeline-499418-data/spark/process_trips.py \
    --region=us-east1 --project=citibike-pipeline-499418
```

### Run dbt
```bash
cd dbt
..\dbt-venv\Scripts\dbt run
..\dbt-venv\Scripts\dbt docs generate
..\dbt-venv\Scripts\dbt docs serve
```

### Deploy Cloud Functions and Scheduler
```powershell
.\deploy_scheduler.ps1
```
