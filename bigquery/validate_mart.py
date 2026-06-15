from google.cloud import bigquery

client = bigquery.Client(project="citibike-pipeline-499418")

# Top 10 worst supply gap stations
query = """
SELECT
    station_name,
    hour_of_day,
    total_departures,
    round(avg_bikes_available, 1)    as avg_bikes,
    round(avg_availability_ratio, 3) as avail_ratio,
    supply_gap_score,
    gap_severity
FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
ORDER BY supply_gap_score DESC
LIMIT 10
"""

rows = list(client.query(query).result())
print(f"\nTop 10 stations by Supply Gap Score")
print(f"{'Station':<45} {'Hr':>3} {'Departs':>8} {'Avail':>7} {'Score':>8} Severity")
print("-" * 92)
for r in rows:
    print(f"{r.station_name:<45} {r.hour_of_day:>3} {r.total_departures:>8} "
          f"{r.avail_ratio:>7.3f} {r.supply_gap_score:>8.1f} {r.gap_severity}")

# Count by severity
count_query = """
SELECT gap_severity, count(*) as cnt
FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
GROUP BY 1
ORDER BY cnt DESC
"""
print("\nRows by severity:")
for r in client.query(count_query).result():
    print(f"  {r.gap_severity}: {r.cnt:,}")

# Total unique stations covered
stations_query = """
SELECT
    count(*) as total_rows,
    count(distinct station_name) as unique_stations,
    count(distinct hour_of_day)  as hours_covered,
    sum(case when avg_bikes_available is not null then 1 end) as rows_with_availability
FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
"""
for r in client.query(stations_query).result():
    print(f"\nSummary: {r.total_rows:,} total rows, {r.unique_stations:,} stations, "
          f"{r.hours_covered} hours, {r.rows_with_availability:,} rows with GBFS data")
