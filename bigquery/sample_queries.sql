-- =============================================================================
--  sample_queries.sql
--  Ready-to-run BigQuery SQL queries that answer the business question.
--  Run these in the BigQuery Console after the pipeline has populated data.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1: TOP 10 STATIONS WITH WORST SUPPLY GAP (all hours combined)
--
-- Shows which stations are chronically under-supplied relative to demand.
-- Perfect for presentations: "These are NYC's most problematic stations."
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    station_name,
    station_id,
    ROUND(SUM(supply_gap_score), 0)         AS total_gap_score,
    SUM(total_departures)                   AS total_historical_trips,
    ROUND(AVG(avg_availability_ratio), 3)   AS avg_availability_all_hours,
    ROUND(AVG(pct_time_near_empty), 3)      AS avg_pct_time_near_empty,
    MAX(gap_severity)                       AS worst_severity
FROM
    `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
WHERE
    is_peak_gap_hour = FALSE   -- Aggregate across all hours
    OR is_peak_gap_hour = TRUE -- Include all rows for this aggregation
GROUP BY
    station_name, station_id
ORDER BY
    total_gap_score DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2: WHEN DOES THE SUPPLY GAP PEAK BY HOUR OF DAY?
--
-- Answers: "Which hours of the day are worst across ALL stations?"
-- Expected pattern: 8-9am (morning commute) and 5-7pm (evening commute).
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    hour_of_day,
    CASE
        WHEN hour_of_day BETWEEN 6 AND 9   THEN 'Morning Commute'
        WHEN hour_of_day BETWEEN 10 AND 15 THEN 'Midday'
        WHEN hour_of_day BETWEEN 16 AND 20 THEN 'Evening Commute'
        ELSE 'Off-Peak'
    END                                         AS time_period,
    ROUND(AVG(supply_gap_score), 2)             AS avg_supply_gap,
    ROUND(SUM(total_departures), 0)             AS total_departures_system_wide,
    ROUND(AVG(avg_availability_ratio), 3)       AS avg_availability,
    COUNT(DISTINCT station_id)                  AS stations_observed
FROM
    `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
GROUP BY
    hour_of_day
ORDER BY
    avg_supply_gap DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3: WORST STATION × HOUR COMBINATIONS (the specific "crisis moments")
--
-- Gives the most actionable insight:
-- "W 21 St & 6 Ave is empty 87% of the time at 8am — with 2,400 daily riders."
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    overall_gap_rank                        AS `rank`,
    station_name,
    hour_of_day,
    start_day_name                          AS day_of_week,
    total_departures,
    ROUND(avg_bikes_available, 1)           AS avg_bikes_available,
    total_capacity,
    ROUND(avg_availability_ratio * 100, 1) AS availability_pct,
    ROUND(pct_time_near_empty * 100, 1)    AS pct_time_near_empty,
    ROUND(supply_gap_score, 0)              AS supply_gap_score,
    gap_severity
FROM
    `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
WHERE
    gap_severity IN ('CRITICAL', 'HIGH')
ORDER BY
    supply_gap_score DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4: MEMBER vs CASUAL RIDER DEMAND SPLIT AT HIGH-GAP STATIONS
--
-- Helps understand WHO drives the demand at problem stations.
-- Commuters (members) vs. tourists (casual) have very different patterns.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    station_name,
    hour_of_day,
    total_departures,
    member_departures,
    casual_departures,
    ROUND(member_departures / NULLIF(total_departures, 0) * 100, 1) AS member_pct,
    ROUND(casual_departures / NULLIF(total_departures, 0) * 100, 1) AS casual_pct,
    supply_gap_score
FROM
    `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`
WHERE
    overall_gap_rank <= 20  -- Top 20 worst supply gap situations
ORDER BY
    supply_gap_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 5: DATA QUALITY CHECK
--
-- Always include a data quality slide in presentations!
-- Shows how much data we have and when it was last updated.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT station_id)              AS total_stations,
    COUNT(DISTINCT hour_of_day)             AS hours_covered,
    SUM(total_departures)                   AS total_trips_analyzed,
    SUM(snapshot_count)                     AS total_availability_snapshots,
    MIN(avg_availability_ratio)             AS min_avg_availability,
    MAX(avg_availability_ratio)             AS max_avg_availability,
    COUNTIF(gap_severity = 'CRITICAL')      AS critical_station_hours,
    COUNTIF(gap_severity = 'HIGH')          AS high_station_hours
FROM
    `citibike-pipeline-499418.citibike_marts.mart_supply_gap_analysis`;
