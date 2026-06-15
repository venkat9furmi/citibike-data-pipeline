"""
create_external_tables.py
Creates BigQuery external tables that point to files in GCS.
External tables let BigQuery query GCS files directly — no data loading needed.
"""
from google.cloud import bigquery

PROJECT  = "citibike-pipeline-499418"
BUCKET   = "citibike-pipeline-499418-data"
client   = bigquery.Client(project=PROJECT)

def create_or_replace(table_id, ext_config):
    table = bigquery.Table(table_id)
    table.external_data_configuration = ext_config
    try:
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)
        print(f"  [OK] {table_id}")
    except Exception as e:
        print(f"  [ERROR] {table_id}: {e}")

# ── 1. station_status_json — real-time GBFS snapshots (NDJSON) ────────────────
# Each file is NDJSON: one station per line with snapshot_at timestamp.
# We list today's folder explicitly — add more date paths as data accumulates.
from datetime import datetime, timezone
today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
print(f"Creating station_status_json external table (path: {today})...")
status_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
status_config.source_uris = [
    f"gs://{BUCKET}/raw/gbfs/station_status/{today}/*.json"
]
status_config.autodetect = True
create_or_replace(f"{PROJECT}.citibike_raw.station_status_json", status_config)

# ── 2. trips_parquet — Spark-processed trip history ───────────────────────────
# Spark writes year=YYYY/month=M hive-partitioned Parquet to processed/trips/.
# Only create this table AFTER Spark has written at least one Parquet file.
# The create_or_replace call is guarded — it will print an error if empty.
print("Creating trips_parquet external table...")
trips_config = bigquery.ExternalConfig("PARQUET")
trips_config.source_uris = [
    f"gs://{BUCKET}/processed/trips/year=*/month=*/*.parquet"
]
trips_config.autodetect = True
hive_opts = bigquery.external_config.HivePartitioningOptions()
hive_opts.mode = "CUSTOM"
hive_opts.source_uri_prefix = f"gs://{BUCKET}/processed/trips/"
hive_opts.fields = ["year", "month"]
trips_config.hive_partitioning = hive_opts
create_or_replace(f"{PROJECT}.citibike_raw.trips_parquet", trips_config)

# ── Verify ─────────────────────────────────────────────────────────────────────
print("\nTables in citibike_raw:")
for t in client.list_tables(f"{PROJECT}.citibike_raw"):
    print(f"  {t.table_id}  ({t.table_type})")
