"""
fetch_trip_history.py
=====================
PURPOSE:
    Download Citi Bike monthly trip CSV files from the public S3 bucket
    and upload them into Google Cloud Storage (GCS) under raw/trip_history/.

HOW IT WORKS:
    1. Parse the S3 index HTML page to discover all CSV filenames.
    2. Filter to only NYC files (files with "JC-" are Jersey City — we skip them).
    3. For each CSV file, check if it already exists in GCS (idempotent).
    4. If not present, stream-download the CSV and upload to GCS.
    5. Log every action so we have an audit trail.

WHY STREAMING?
    Some monthly files are 500 MB+. Streaming avoids loading the full
    file into RAM — we pipe bytes directly from S3 to GCS.

RUN:
    python fetch_trip_history.py --months 12
    python fetch_trip_history.py --all          # Full historical backfill
"""

import argparse
import io
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup
from google.cloud import storage
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Load central config ────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH) as f:
    CFG = yaml.safe_load(f)

PROJECT_ID   = CFG["gcp"]["project_id"]
BUCKET_NAME  = CFG["gcs"]["bucket_name"]
RAW_PREFIX   = CFG["gcs"]["raw_trip_prefix"]       # "raw/trip_history"
S3_INDEX_URL = CFG["sources"]["trip_history_base_url"]  # https://s3.amazonaws.com/tripdata/

# ── GCS client ────────────────────────────────────────────────────────────────
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


def parse_s3_index() -> list[str]:
    """
    Fetch the S3 index page and return all CSV/zip filenames.

    The S3 index is a plain HTML page with <Key> elements listing filenames
    like '202401-citibike-tripdata.csv.zip'.
    """
    logger.info(f"Fetching S3 index from {S3_INDEX_URL}")
    resp = requests.get(S3_INDEX_URL, timeout=30)
    resp.raise_for_status()

    # S3 returns XML — use the xml parser so <Key> tags are found correctly
    soup = BeautifulSoup(resp.text, features="xml")

    # Each file is listed in a <Key> tag (capital K in S3 XML)
    keys = [tag.text.strip() for tag in soup.find_all("Key")]

    # Keep only NYC trip data files (skip Jersey City "JC-" prefix and index.html)
    # File formats seen on S3: 202605-citibike-tripdata.zip  /  202401-citibike-tripdata.csv.zip
    nyc_files = [
        k for k in keys
        if "citibike-tripdata" in k or "citibike_tripdata" in k
        and not k.startswith("JC-")
        and k != "index.html"
        and (k.endswith(".zip") or k.endswith(".csv"))
    ]

    logger.info(f"Found {len(nyc_files)} NYC trip history files on S3")
    return nyc_files


def filter_recent_months(all_files: list[str], months: int) -> list[str]:
    """
    Return only files from the last N months.
    File names start with YYYYMM, e.g. '202605-citibike-tripdata.zip'
    Comparison is at month granularity — cutoff is the 1st of the cutoff month.
    """
    cutoff_raw = datetime.now() - timedelta(days=30 * months)
    cutoff = cutoff_raw.replace(day=1)   # compare from start of month, not mid-month
    filtered = []
    for fname in all_files:
        try:
            year_month = fname[:6]          # e.g. "202605"
            file_date = datetime.strptime(year_month, "%Y%m")
            if file_date >= cutoff:
                filtered.append(fname)
        except ValueError:
            continue  # skip files with non-YYYYMM prefix (e.g. "2013-citibike...")
    logger.info(f"Filtered to {len(filtered)} files within last {months} months")
    return filtered


def gcs_blob_exists(blob_name: str) -> bool:
    """Check if a file already exists in GCS — prevents duplicate uploads."""
    blob = bucket.blob(blob_name)
    return blob.exists()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=10, max=60))
def stream_upload_to_gcs(s3_filename: str) -> None:
    """
    Download a large zip file from S3 in 8 MB chunks to a temp file,
    then upload from disk to GCS. Avoids loading hundreds of MB into RAM.
    """
    gcs_blob_name = f"{RAW_PREFIX}/{s3_filename}"

    if gcs_blob_exists(gcs_blob_name):
        logger.info(f"[SKIP] Already in GCS: {gcs_blob_name}")
        return

    s3_url = f"{S3_INDEX_URL}{s3_filename}"
    logger.info(f"[DOWNLOAD] {s3_url}")

    tmp_path = None
    try:
        # Download in 8 MB chunks to a temporary file on disk
        with requests.get(s3_url, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            logger.info(f"  File size: {total / 1024 / 1024:.1f} MB")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp_path = tmp.name
                downloaded = 0
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                        downloaded += len(chunk)
                        pct = (downloaded / total * 100) if total else 0
                        logger.info(f"  Progress: {downloaded / 1024 / 1024:.0f} MB ({pct:.0f}%)")

        # Upload the temp file to GCS (resumable upload, handles large files)
        logger.info(f"[UPLOAD] Sending to GCS...")
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(
            tmp_path,
            content_type="application/octet-stream",
            timeout=600
        )
        logger.success(f"[UPLOADED] gs://{BUCKET_NAME}/{gcs_blob_name}")

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)  # Always clean up temp file


def main():
    parser = argparse.ArgumentParser(description="Ingest Citi Bike trip history to GCS")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--months", type=int, default=12,
                       help="How many recent months to ingest (default: 12)")
    group.add_argument("--all", action="store_true",
                       help="Ingest all available historical data")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Citi Bike Trip History Ingestion Starting")
    logger.info(f"  GCP Project : {PROJECT_ID}")
    logger.info(f"  GCS Bucket  : {BUCKET_NAME}")
    logger.info("=" * 60)

    all_files = parse_s3_index()

    if args.all:
        files_to_process = all_files
        logger.info("Mode: Full historical backfill")
    else:
        files_to_process = filter_recent_months(all_files, args.months)
        logger.info(f"Mode: Last {args.months} months")

    success_count = 0
    error_count = 0

    for fname in files_to_process:
        try:
            stream_upload_to_gcs(fname)
            success_count += 1
        except Exception as e:
            logger.error(f"[FAILED] {fname}: {e}")
            error_count += 1

    logger.info("=" * 60)
    logger.info(f"Ingestion complete. Success: {success_count} | Errors: {error_count}")
    logger.info("=" * 60)

    if error_count > 0:
        sys.exit(1)  # Non-zero exit code alerts Cloud Scheduler of failure


if __name__ == "__main__":
    main()
