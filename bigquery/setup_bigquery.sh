#!/bin/bash
# =============================================================================
#  setup_bigquery.sh
#  Creates all BigQuery datasets and tables for the Citi Bike pipeline.
#
#  WHAT IS BIGQUERY?
#    BigQuery is Google's serverless, columnar data warehouse.
#    You don't manage servers — you just load data and run SQL.
#    It charges per TB scanned, not per hour the cluster runs.
#
#  RUN ONCE:
#    bash bigquery/setup_bigquery.sh
# =============================================================================

PROJECT="citibike-pipeline-499418"
REGION="us-east1"
BUCKET="citibike-pipeline-499418-data"

echo "=============================================="
echo "  Setting up BigQuery for project: $PROJECT"
echo "=============================================="

# Set the active GCP project
gcloud config set project $PROJECT

# ── STEP 1: Create GCS bucket ─────────────────────────────────────────────────
# The bucket stores ALL raw and processed data
echo ""
echo "[1/5] Creating GCS data bucket..."
gcloud storage buckets create gs://$BUCKET \
    --project=$PROJECT \
    --location=$REGION \
    --uniform-bucket-level-access \
    2>/dev/null || echo "  Bucket already exists — skipping."

# Spark logs bucket (Dataproc Serverless requires this)
gcloud storage buckets create gs://${PROJECT}-spark-logs \
    --project=$PROJECT \
    --location=$REGION \
    2>/dev/null || echo "  Spark logs bucket already exists — skipping."

# ── STEP 2: Create BigQuery datasets ──────────────────────────────────────────
# A dataset in BigQuery is like a schema/database in traditional SQL.
echo ""
echo "[2/5] Creating BigQuery datasets..."

# RAW dataset: external tables that point directly to GCS Parquet files
bq --location=$REGION mk \
    --dataset \
    --description="Raw Citi Bike data — external tables over GCS" \
    $PROJECT:citibike_raw 2>/dev/null || echo "  citibike_raw already exists."

# STAGING dataset: dbt staging models (lightly cleaned raw data)
bq --location=$REGION mk \
    --dataset \
    --description="dbt staging layer — cleaned and typed" \
    $PROJECT:citibike_staging 2>/dev/null || echo "  citibike_staging already exists."

# MARTS dataset: final business-ready tables (answers the business question)
bq --location=$REGION mk \
    --dataset \
    --description="dbt mart layer — business-ready analytical tables" \
    $PROJECT:citibike_marts 2>/dev/null || echo "  citibike_marts already exists."

# ── STEP 3: Create external table over Spark-processed Parquet ────────────────
# An EXTERNAL table in BigQuery means the data stays in GCS.
# BigQuery reads it on-the-fly when you query — no data loading needed!
# The hive_partition_uri_prefix tells BigQuery about year=/month= partitions.
echo ""
echo "[3/5] Creating external table: citibike_raw.trips_parquet ..."
bq mk --external_table_definition=/dev/stdin \
    $PROJECT:citibike_raw.trips_parquet << 'EOF'
{
  "sourceFormat": "PARQUET",
  "sourceUris": ["gs://citibike-pipeline-499418-data/processed/trips/*.parquet",
                 "gs://citibike-pipeline-499418-data/processed/trips/**/*.parquet"],
  "hivePartitioningOptions": {
    "mode": "AUTO",
    "sourceUriPrefix": "gs://citibike-pipeline-499418-data/processed/trips/"
  },
  "autodetect": true
}
EOF
echo "  Done."

# ── STEP 4: Create external table for GBFS station status JSON ────────────────
# We store real-time GBFS snapshots as JSON in GCS.
# BigQuery can query JSON files natively using JSON_EXTRACT functions.
echo ""
echo "[4/5] Creating external table: citibike_raw.station_status_json ..."
bq mk --external_table_definition=/dev/stdin \
    $PROJECT:citibike_raw.station_status_json << 'EOF'
{
  "sourceFormat": "NEWLINE_DELIMITED_JSON",
  "sourceUris": ["gs://citibike-pipeline-499418-data/raw/gbfs/station_status/**/*.json"],
  "autodetect": true
}
EOF
echo "  Done."

# ── STEP 5: Create native BigQuery table for station reference data ────────────
# Station info (name, lat, lon, capacity) is small enough to load natively.
# Native tables are faster to query than external tables.
echo ""
echo "[5/5] Creating native table: citibike_raw.station_information ..."
bq mk \
    --table \
    --description="Station reference data from GBFS station_information.json" \
    $PROJECT:citibike_raw.station_information \
    station_id:STRING,name:STRING,short_name:STRING,lat:FLOAT64,lon:FLOAT64,\
capacity:INTEGER,region_id:STRING,_ingested_at:TIMESTAMP \
    2>/dev/null || echo "  Table already exists."

echo ""
echo "=============================================="
echo "  BigQuery setup complete!"
echo "  Datasets created:"
echo "    - $PROJECT:citibike_raw"
echo "    - $PROJECT:citibike_staging"
echo "    - $PROJECT:citibike_marts"
echo "=============================================="
