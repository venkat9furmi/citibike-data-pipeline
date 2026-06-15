"""
Load GBFS station_information.json from GCS into BigQuery native table.
Also loads station names so we can join across both data systems.
"""
import json
from google.cloud import storage, bigquery
from datetime import datetime, timezone

PROJECT = "citibike-pipeline-499418"
BUCKET  = "citibike-pipeline-499418-data"

storage_client = storage.Client(project=PROJECT)
bq_client      = bigquery.Client(project=PROJECT)

# Download station_information.json from GCS
blob = storage_client.bucket(BUCKET).blob("raw/gbfs/station_info/station_information.json")
data = json.loads(blob.download_as_text())
stations = data["data"]["stations"]
ingested_at = datetime.now(timezone.utc).isoformat()

# Build rows for BigQuery
rows = []
for s in stations:
    rows.append({
        "station_id":   str(s.get("station_id", "")),
        "name":         s.get("name", ""),
        "short_name":   s.get("short_name", ""),
        "lat":          float(s.get("lat", 0)),
        "lon":          float(s.get("lon", 0)),
        "capacity":     int(s.get("capacity", 0)),
        "region_id":    str(s.get("region_id", "")),
        "_ingested_at": ingested_at,
    })

# Clear existing rows and insert fresh
table_ref = f"{PROJECT}.citibike_raw.station_information"
bq_client.delete_table(table_ref, not_found_ok=True)

schema = [
    bigquery.SchemaField("station_id",   "STRING"),
    bigquery.SchemaField("name",         "STRING"),
    bigquery.SchemaField("short_name",   "STRING"),
    bigquery.SchemaField("lat",          "FLOAT64"),
    bigquery.SchemaField("lon",          "FLOAT64"),
    bigquery.SchemaField("capacity",     "INTEGER"),
    bigquery.SchemaField("region_id",    "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]
table = bigquery.Table(table_ref, schema=schema)
bq_client.create_table(table)

errors = bq_client.insert_rows_json(table_ref, rows)
if errors:
    print(f"Errors: {errors}")
else:
    print(f"Loaded {len(rows)} stations into {table_ref}")
