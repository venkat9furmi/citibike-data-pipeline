-- =============================================================================
--  stg_station_status.sql  (Staging Model)
-- =============================================================================
--  SOURCE:
--    citibike_raw.station_status_json — external BigQuery table over NDJSON.
--    Each row IS one station at one snapshot (flat — no unnesting needed).
--    Fields come directly from GBFS + our snapshot_at timestamp.
--
--  GBFS fields available per row:
--    station_id, num_bikes_available, num_ebikes_available,
--    num_docks_available, is_installed, is_renting, is_returning,
--    num_bikes_disabled, last_reported, snapshot_at, snapshot_epoch
-- =============================================================================

with source as (

    select * from {{ source('citibike_raw', 'station_status_json') }}

),

cleaned as (

    select
        -- Snapshot timestamp (when we captured this reading)
        timestamp(snapshot_at)                                              as snapshot_at,
        extract(hour from timestamp(snapshot_at))                           as snapshot_hour,
        date(timestamp(snapshot_at))                                        as snapshot_date,

        -- Station identifier
        cast(station_id as string)                                          as station_id,

        -- Availability metrics
        cast(num_bikes_available  as int64)                                 as num_bikes_available,
        cast(num_ebikes_available as int64)                                 as num_ebikes_available,
        cast(num_docks_available  as int64)                                 as num_docks_available,
        cast(num_bikes_disabled   as int64)                                 as num_bikes_disabled,

        -- Station operational flags
        cast(is_installed  as int64)                                        as is_installed,
        cast(is_renting    as int64)                                        as is_renting,
        cast(is_returning  as int64)                                        as is_returning

    from source

    where
        station_id  is not null
        and snapshot_at is not null
        and cast(is_installed as int64) = 1

)

select * from cleaned
