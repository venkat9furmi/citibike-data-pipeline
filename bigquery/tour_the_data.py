from google.cloud import bigquery, storage

PROJECT = "citibike-pipeline-499418"
BUCKET  = "citibike-pipeline-499418-data"
bq      = bigquery.Client(project=PROJECT)
gcs     = storage.Client(project=PROJECT)

def q(sql):
    return list(bq.query(sql).result())

print("=" * 60)
print("  CITI BIKE PIPELINE — FULL DATA TOUR")
print("=" * 60)

# ── GCS ─────────────────────────────────────────────────────
print("\n[1] GOOGLE CLOUD STORAGE (GCS)")
print("    Bucket: citibike-pipeline-499418-data")

blobs = list(gcs.bucket(BUCKET).list_blobs())
folders = {}
for b in blobs:
    top = b.name.split("/")[0] + "/" + b.name.split("/")[1] if "/" in b.name else b.name
    folders[top] = folders.get(top, {"count": 0, "bytes": 0})
    folders[top]["count"] += 1
    folders[top]["bytes"] += b.size

for folder, info in sorted(folders.items()):
    mb = info["bytes"] / 1024 / 1024
    print(f"    gs://{BUCKET}/{folder}  →  {info['count']} file(s), {mb:.1f} MB")

# ── RAW LAYER ───────────────────────────────────────────────
print("\n[2] BIGQUERY — DATASET: citibike_raw  (raw / external tables)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_raw.trips_parquet`")
print(f"    trips_parquet         → {rows[0].n:>10,} rows  (external → GCS Parquet)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_raw.station_status_json`")
print(f"    station_status_json   → {rows[0].n:>10,} rows  (external → GCS NDJSON)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_raw.station_information`")
print(f"    station_information   → {rows[0].n:>10,} rows  (native table)")

# ── STAGING LAYER ────────────────────────────────────────────
print("\n[3] BIGQUERY — DATASET: citibike_staging  (dbt staging views)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_staging.stg_trip_history`")
print(f"    stg_trip_history      → {rows[0].n:>10,} rows  (view — cleaned trips)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_staging.stg_station_status`")
print(f"    stg_station_status    → {rows[0].n:>10,} rows  (view — cleaned GBFS status)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_staging.stg_station_information`")
print(f"    stg_station_information → {rows[0].n:>8,} rows  (view — station reference)")

# ── INTERMEDIATE LAYER ───────────────────────────────────────
print("\n[4] BIGQUERY — DATASET: citibike_staging  (dbt intermediate views)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_staging.int_station_hourly_demand`")
print(f"    int_station_hourly_demand   → {rows[0].n:>6,} rows  (demand by station+hour)")

rows = q("SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_staging.int_station_avg_availability`")
print(f"    int_station_avg_availability→ {rows[0].n:>6,} rows  (availability by station+hour)")

# ── MART LAYER ───────────────────────────────────────────────
print("\n[5] BIGQUERY — DATASET: citibike_marts  (final business table)")

rows = q("""
SELECT count(*) as total_rows,
       count(distinct station_name) as stations,
       count(distinct hour_of_day) as hours,
       countif(gap_severity = 'CRITICAL') as critical,
       countif(gap_severity = 'HIGH') as high,
       countif(gap_severity = 'MEDIUM') as medium,
       countif(gap_severity = 'LOW') as low
FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
""")
r = rows[0]
print(f"    mart_supply_gap_analysis → {r.total_rows:>6,} rows")
print(f"      ├── Unique stations  : {r.stations:,}")
print(f"      ├── Hours covered    : {r.hours} (need more GBFS snapshots for 24h)")
print(f"      └── Gap severity     : {r.critical} CRITICAL / {r.high} HIGH / {r.medium} MEDIUM / {r.low} LOW")

# ── SAMPLE: TOP 5 GAP STATIONS ───────────────────────────────
print("\n[6] SAMPLE — Top 5 worst supply gap stations")
rows = q("""
SELECT station_name, hour_of_day,
       total_departures,
       round(avg_availability_ratio * 100, 1) as pct_bikes_available,
       supply_gap_score, gap_severity
FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
WHERE is_peak_gap_hour = true
ORDER BY supply_gap_score DESC
LIMIT 5
""")
for r in rows:
    print(f"    {r.station_name:<45} hr={r.hour_of_day}  "
          f"departures={r.total_departures}  "
          f"bikes_avail={r.pct_bikes_available}%  "
          f"gap={r.supply_gap_score}  [{r.gap_severity}]")

# ── TRIP SAMPLE ──────────────────────────────────────────────
print("\n[7] SAMPLE — 3 rows from raw trips table")
rows = q("""
SELECT ride_id, started_at_ts, start_station_name, end_station_name,
       trip_duration_minutes, member_casual
FROM `citibike-pipeline-499418.citibike_raw.trips_parquet`
LIMIT 3
""")
for r in rows:
    print(f"    {r.ride_id[:12]}..  {str(r.started_at_ts)[:16]}  "
          f"{r.start_station_name[:30]:<30} → {r.end_station_name[:25]:<25}  "
          f"{r.trip_duration_minutes:.0f}min  {r.member_casual}")

print("\n" + "=" * 60)
print("  PIPELINE COMPLETE ✓")
print("=" * 60)
