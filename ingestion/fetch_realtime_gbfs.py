"""
fetch_realtime_gbfs.py
======================
PURPOSE:
    Fetch two real-time GBFS (General Bikeshare Feed Specification) endpoints:
      1. station_status.json  — bikes available RIGHT NOW at each station
      2. station_information.json — static metadata (name, lat/lon, capacity)

    Save each snapshot as a timestamped JSON file in GCS so we build
    a time-series of availability — this lets us answer "when does the
    supply gap peak by hour of day?"

HOW IT WORKS:
    - Called every 5 minutes by Cloud Scheduler.
    - Each call produces two JSON files in GCS:
        raw/gbfs/station_status/2024/01/15/station_status_20240115_143022.json
        raw/gbfs/station_info/station_information.json  (rarely changes, overwrites)

    - The station_status snapshots accumulate over time — they become our
      real-time availability history that we join against trip demand.

WHY GBFS?
    GBFS is an open standard adopted by 500+ bikeshare systems worldwide.
    The data refreshes every 30 seconds on Citi Bike's side.

RUN:
    python fetch_realtime_gbfs.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from google.cloud import storage
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Load config ────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH) as f:
    CFG = yaml.safe_load(f)

PROJECT_ID        = CFG["gcp"]["project_id"]
BUCKET_NAME       = CFG["gcs"]["bucket_name"]
GBFS_PREFIX       = CFG["gcs"]["raw_gbfs_prefix"]          # "raw/gbfs"
STATUS_URL        = CFG["sources"]["gbfs_station_status_url"]
INFO_URL          = CFG["sources"]["gbfs_station_info_url"]

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_json(url: str) -> dict:
    """Fetch a JSON endpoint with retry on network errors."""
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def upload_json_to_gcs(data: dict, blob_name: str) -> None:
    """Serialize dict to JSON and upload to GCS."""
    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json_bytes, content_type="application/json")
    logger.success(f"[UPLOADED] gs://{BUCKET_NAME}/{blob_name}")


def ingest_station_status() -> None:
    """
    Fetch real-time station status and store as NDJSON (one station per line).

    BigQuery external tables require NEWLINE_DELIMITED_JSON format — each line
    must be a complete, flat JSON object. We flatten the nested GBFS response
    so each station becomes its own line with the snapshot timestamp attached.

    Output line example:
    {"station_id":"72","num_bikes_available":3,"snapshot_at":"2026-06-15T14:30:00Z"}
    """
    logger.info(f"Fetching station status from {STATUS_URL}")
    data = fetch_json(STATUS_URL)

    now_utc = datetime.now(timezone.utc)
    snapshot_at = now_utc.isoformat()
    snapshot_epoch = int(now_utc.timestamp())

    stations = data.get("data", {}).get("stations", [])
    logger.info(f"Fetched status for {len(stations)} stations")

    # Build NDJSON: one flat JSON object per line per station
    lines = []
    for s in stations:
        row = dict(s)                         # copy all GBFS fields
        row["snapshot_at"]    = snapshot_at   # add our timestamp
        row["snapshot_epoch"] = snapshot_epoch
        lines.append(json.dumps(row))

    ndjson_bytes = "\n".join(lines).encode("utf-8")

    # Partition path: raw/gbfs/station_status/YYYY/MM/DD/
    date_path    = now_utc.strftime("%Y/%m/%d")
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
    blob_name = f"{GBFS_PREFIX}/station_status/{date_path}/station_status_{timestamp_str}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(ndjson_bytes, content_type="application/json")
    logger.success(f"[UPLOADED] gs://{BUCKET_NAME}/{blob_name} ({len(stations)} rows)")


def ingest_station_information() -> None:
    """
    Fetch static station metadata (name, location, capacity).

    Station info rarely changes — new stations are added maybe once a month.
    We overwrite the same file each run so there's always a current reference.

    The GBFS station_information response includes:
      station_id, name, short_name, lat, lon, capacity, region_id
    """
    logger.info(f"Fetching station information from {INFO_URL}")
    data = fetch_json(INFO_URL)

    now_utc = datetime.now(timezone.utc)
    data["_ingested_at"] = now_utc.isoformat()

    station_count = len(data.get("data", {}).get("stations", []))
    logger.info(f"Fetched info for {station_count} stations")

    # Overwrite — we always want the latest station list
    blob_name = f"{GBFS_PREFIX}/station_info/station_information.json"
    upload_json_to_gcs(data, blob_name)


def main():
    logger.info("=" * 60)
    logger.info("Citi Bike GBFS Real-Time Ingestion Starting")
    logger.info(f"  Project: {PROJECT_ID}")
    logger.info(f"  Bucket : {BUCKET_NAME}")
    logger.info("=" * 60)

    ingest_station_status()
    ingest_station_information()

    logger.info("GBFS ingestion complete.")


if __name__ == "__main__":
    main()
