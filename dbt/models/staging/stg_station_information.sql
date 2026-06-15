-- =============================================================================
--  stg_station_information.sql  (Staging Model)
-- =============================================================================
--  PURPOSE:
--    Clean the station reference table loaded from GBFS station_information.json.
--    This gives us station names, geographic coordinates, and dock capacity —
--    needed to join with trip data and status data.
--
--  SOURCE:
--    citibike_raw.station_information — native BigQuery table (small reference data)
-- =============================================================================

with source as (

    select * from {{ source('citibike_raw', 'station_information') }}

),

cleaned as (

    select
        station_id,
        name                                as station_name,
        short_name                          as station_short_name,

        -- Geographic coordinates (useful for map visualizations)
        cast(lat as float64)                as latitude,
        cast(lon as float64)                as longitude,

        -- Total dock capacity — used to calculate utilization %
        cast(capacity as int64)             as total_capacity,

        region_id,

        -- When this record was ingested
        _ingested_at                        as info_loaded_at

    from source

    where
        station_id  is not null
        and name        is not null
        and capacity    > 0

)

select * from cleaned
