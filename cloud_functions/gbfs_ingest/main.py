"""
Cloud Function: gbfs_ingest
===========================
Triggered by Cloud Scheduler every 15 minutes via HTTP.
Fetches Citi Bike real-time station status from the GBFS API
and writes a flat NDJSON snapshot to GCS.

Each snapshot accumulates to build a time-series of bike availability
that we join against historical trip demand in the mart.
"""

import json
import functions_framework
from datetime import datetime, timezone
import requests
from google.cloud import storage

PROJECT_ID   = "citibike-pipeline-499418"
BUCKET_NAME  = "citibike-pipeline-499418-data"
STATUS_URL   = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json"
INFO_URL     = "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json"

storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(BUCKET_NAME)


def fetch_json(url: str) -> dict:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def ingest_station_status(now_utc: datetime) -> int:
    """Fetch station status, write flat NDJSON to GCS. Returns station count."""
    data     = fetch_json(STATUS_URL)
    stations = data.get("data", {}).get("stations", [])

    snapshot_at    = now_utc.isoformat()
    snapshot_epoch = int(now_utc.timestamp())

    lines = []
    for s in stations:
        row = dict(s)
        row["snapshot_at"]    = snapshot_at
        row["snapshot_epoch"] = snapshot_epoch
        lines.append(json.dumps(row))

    ndjson_bytes  = "\n".join(lines).encode("utf-8")
    date_path     = now_utc.strftime("%Y/%m/%d")
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
    blob_name     = f"raw/gbfs/station_status/{date_path}/station_status_{timestamp_str}.json"

    bucket.blob(blob_name).upload_from_string(ndjson_bytes, content_type="application/json")
    return len(stations)


def ingest_station_information(now_utc: datetime) -> None:
    """Fetch static station metadata, overwrite the reference file in GCS."""
    data = fetch_json(INFO_URL)
    data["_ingested_at"] = now_utc.isoformat()
    blob_name = "raw/gbfs/station_info/station_information.json"
    bucket.blob(blob_name).upload_from_string(
        json.dumps(data, indent=2).encode("utf-8"),
        content_type="application/json"
    )


@functions_framework.http
def gbfs_ingest(request):
    """HTTP entry point called by Cloud Scheduler."""
    try:
        now_utc = datetime.now(timezone.utc)
        n       = ingest_station_status(now_utc)
        ingest_station_information(now_utc)
        msg = f"OK: {n} stations at {now_utc.isoformat()}"
        print(msg)
        return (msg, 200)
    except Exception as e:
        print(f"ERROR: {e}")
        return (f"Error: {e}", 500)
