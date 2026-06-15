-- =============================================================================
--  stg_trip_history.sql  (Staging Model)
-- =============================================================================
--  PURPOSE:
--    Clean and standardize the raw Parquet trip data loaded by Spark.
--    This is the FIRST transformation layer — we only rename, cast, and
--    filter here. No business logic yet.
--
--  SOURCE:
--    citibike_raw.trips_parquet — external BigQuery table over GCS Parquet files
--    created by our Spark job.
--
--  OUTPUT:
--    citibike_staging.stg_trip_history (materialized as VIEW)
--
--  KEY DECISIONS:
--    - We filter rides shorter than 1 min or longer than 12 hours here as well
--      (belt-and-suspenders — Spark already filtered, but raw tables can have
--       new files that bypassed Spark).
--    - trip_date is cast to DATE for partitioning in downstream models.
-- =============================================================================

with source as (

    select * from {{ source('citibike_raw', 'trips_parquet') }}

),

cleaned as (

    select
        -- Identifiers
        ride_id,
        rideable_type,

        -- Timestamps — already cleaned by Spark
        started_at_ts                                       as started_at,
        ended_at_ts                                         as ended_at,

        -- Station fields — standardize naming
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,

        -- Location (lat/lon for geo analysis)
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        -- Rider type: "member" (annual pass) vs "casual" (day/single-trip)
        member_casual,

        -- Duration
        trip_duration_minutes,

        -- Time features (pre-computed by Spark)
        start_hour,
        start_day_of_week,
        start_day_name,
        trip_date,

        -- Partition columns — derived from timestamp (not in Parquet data columns)
        extract(year  from started_at_ts) as year,
        extract(month from started_at_ts) as month

    from source

    where
        -- Remove any remaining invalid rows
        ride_id                   is not null
        and start_station_id      is not null
        and started_at_ts         is not null
        and trip_duration_minutes between 1 and 720

)

select * from cleaned
