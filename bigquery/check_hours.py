from google.cloud import bigquery
c = bigquery.Client(project="citibike-pipeline-499418")

print("Hours covered in mart:")
for r in c.query("""
    SELECT hour_of_day, count(*) as row_count
    FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
    GROUP BY 1 ORDER BY 1
""").result():
    print(f"  Hour {r.hour_of_day:02d}:00  -> {r.row_count:,} rows")

print("\nTop 10 worst supply gaps (worst hour per station):")
for r in c.query("""
    SELECT station_name, hour_of_day, total_departures,
           round(avg_availability_ratio*100,1) as pct_bikes,
           supply_gap_score, gap_severity
    FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
    WHERE is_peak_gap_hour = true
    ORDER BY supply_gap_score DESC LIMIT 10
""").result():
    print(f"  [{r.gap_severity}] {r.station_name:<42} hr={r.hour_of_day:02d}  "
          f"trips={r.total_departures}  bikes={r.pct_bikes}%  score={r.supply_gap_score}")

print("\nTotal rows:", list(c.query(
    "SELECT count(*) as n FROM `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`"
).result())[0].n)
