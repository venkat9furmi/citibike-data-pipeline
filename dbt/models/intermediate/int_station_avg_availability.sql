-- =============================================================================
--  int_station_avg_availability.sql  (Intermediate Model)
-- =============================================================================
--  PURPOSE:
--    Compute AVERAGE REAL-TIME BIKE AVAILABILITY per station per hour of day,
--    using the GBFS snapshots collected every 5 minutes.
--
--    This answers the "supply side" of our business question:
--    "Which stations have the LOWEST real-time bike availability?"
--
--  LOGIC:
--    We have thousands of GBFS snapshots (one every 5 minutes for each station).
--    Group them by station_id and snapshot_hour, then average:
--      - num_bikes_available
--      - availability_ratio = bikes available / total capacity
--
--    availability_ratio close to 0 = station frequently empty = supply problem
--
--  WHY AVERAGE ACROSS HOURS?
--    A single snapshot is noisy. Averaging across many days at the same hour
--    reveals structural patterns — e.g., "Penn Station is always empty at 9am."
-- =============================================================================

with status as (

    select * from {{ ref('stg_station_status') }}

),

info as (

    select * from {{ ref('stg_station_information') }}

),

-- Join status snapshots with station capacity info
status_with_capacity as (

    select
        s.station_id,
        s.snapshot_at,
        s.snapshot_hour,
        s.snapshot_date,
        s.num_bikes_available,
        s.num_ebikes_available,
        s.num_docks_available,
        s.is_renting,

        -- Capacity from the reference table
        i.total_capacity,
        i.station_name,
        i.latitude,
        i.longitude,

        -- Availability ratio: 0 = empty, 1 = full
        safe_divide(s.num_bikes_available, i.total_capacity)    as availability_ratio

    from status s
    left join info i using (station_id)

    where i.total_capacity > 0   -- Exclude stations with unknown capacity

),

-- Aggregate to hourly averages across all snapshot days
hourly_averages as (

    select
        station_id,
        station_name,
        latitude,
        longitude,
        total_capacity,
        snapshot_hour                               as hour_of_day,

        -- Average availability across all observed days at this hour
        round(avg(num_bikes_available), 2)          as avg_bikes_available,
        round(avg(num_ebikes_available), 2)         as avg_ebikes_available,
        round(avg(num_docks_available), 2)          as avg_docks_available,

        -- How often availability ratio is dangerously low (<10% = near-empty)
        round(avg(availability_ratio), 4)           as avg_availability_ratio,
        round(countif(availability_ratio < 0.1) / count(*), 4)
                                                    as pct_time_near_empty,

        -- How often the station is fully stocked (>90%)
        round(countif(availability_ratio > 0.9) / count(*), 4)
                                                    as pct_time_near_full,

        -- Number of snapshots (data quality — more = more reliable average)
        count(*)                                    as snapshot_count

    from status_with_capacity
    group by 1, 2, 3, 4, 5, 6

)

select * from hourly_averages
