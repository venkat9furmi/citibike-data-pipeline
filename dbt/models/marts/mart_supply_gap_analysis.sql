-- =============================================================================
--  mart_supply_gap_analysis.sql  (Mart Model — FINAL BUSINESS TABLE)
-- =============================================================================
--  BUSINESS QUESTION ANSWERED:
--    "Which Citi Bike stations have the highest historical trip demand
--     but lowest real-time bike availability — and when does this
--     supply gap peak by hour of day?"
--
--  HOW THIS MODEL WORKS:
--    1. JOIN historical demand (int_station_hourly_demand)
--         WITH real-time availability (int_station_avg_availability)
--         ON station_id + hour_of_day
--    2. COMPUTE a SUPPLY GAP SCORE:
--         High demand × Low availability = High gap score = Problem station+hour
--    3. RANK stations by their worst supply gap
--    4. This table is what a BI tool (Looker Studio, Tableau) queries directly
--
--  OUTPUT SCHEMA:
--    station_id, station_name, hour_of_day, total_departures,
--    avg_bikes_available, avg_availability_ratio,
--    supply_gap_score, supply_gap_rank, peak_gap_hour_flag
--
--  MATERIALIZED AS:
--    BigQuery table, partitioned by trip_date, clustered by station_id + hour
--    This makes queries like "show me top 10 gap stations at 8am" very fast.
-- =============================================================================

with demand as (

    select * from {{ ref('int_station_hourly_demand') }}

),

availability as (

    select * from {{ ref('int_station_avg_availability') }}

),

-- Collapse availability to one row per station (average across all snapshots/hours).
-- This lets us join against all 24 demand hours, not just the hours we happened
-- to capture a GBFS snapshot for. With only a few snapshots the ratio is still
-- meaningful — it reflects current station fill level across available readings.
availability_by_station as (

    select
        station_name,
        max(station_id)                         as gbfs_station_id,
        max(latitude)                           as latitude,
        max(longitude)                          as longitude,
        max(total_capacity)                     as total_capacity,
        round(avg(avg_bikes_available),  2)     as avg_bikes_available,
        round(avg(avg_ebikes_available), 2)     as avg_ebikes_available,
        round(avg(avg_availability_ratio), 4)   as avg_availability_ratio,
        round(avg(pct_time_near_empty),  4)     as pct_time_near_empty,
        sum(snapshot_count)                     as snapshot_count

    from availability
    group by station_name

),

-- Join demand (all 24 hours) with availability on station NAME only.
-- Trip history uses numeric IDs ("3704.04"); GBFS uses UUIDs — IDs never match.
-- Station names are consistent across both systems, so we join on name.
joined as (

    select
        -- Identifiers
        d.station_id                                as station_id,
        coalesce(d.station_name, a.station_name)    as station_name,
        a.gbfs_station_id,
        d.hour_of_day,
        d.start_day_of_week,
        d.start_day_name,

        -- Geographic info (from availability side which has lat/lon)
        a.latitude,
        a.longitude,
        a.total_capacity,

        -- ── DEMAND METRICS (historical trip data) ──────────────────────
        d.total_departures,
        d.total_arrivals,
        d.net_demand,
        d.demand_to_arrival_ratio,
        d.avg_trip_duration_minutes,
        d.member_departures,
        d.casual_departures,
        d.ebike_departures,

        -- ── SUPPLY METRICS (real-time GBFS snapshots) ──────────────────
        a.avg_bikes_available,
        a.avg_ebikes_available,
        a.avg_availability_ratio,       -- 0=always empty, 1=always full
        a.pct_time_near_empty,          -- How often < 10% bikes available
        a.snapshot_count,               -- Data quality indicator

        -- ── SUPPLY GAP SCORE (THE KEY METRIC) ──────────────────────────
        --
        -- Formula:
        --   supply_gap_score = total_departures × (1 - avg_availability_ratio)
        --
        -- Why this formula?
        --   - total_departures measures how much demand there is
        --   - (1 - avg_availability_ratio) measures how often bikes are MISSING
        --   - Multiplying them gives high scores only when BOTH are true:
        --     station is busy AND frequently empty
        --   - Score = 0 means either no demand OR always has bikes available
        --   - Score = 1000 means high demand AND always empty = worst station
        --
        round(
            d.total_departures * (1 - coalesce(a.avg_availability_ratio, 0)),
            2
        )                               as supply_gap_score

    from demand d
    left join availability_by_station a
        on lower(trim(d.station_name)) = lower(trim(a.station_name))

    -- Only include stations where we have both demand AND availability data
    where
        a.gbfs_station_id   is not null
        and a.snapshot_count >= 1

),

-- Add rankings and flags
ranked as (

    select
        *,

        -- Global rank: #1 = worst supply gap overall across all hours
        row_number() over (
            order by supply_gap_score desc
        )                               as overall_gap_rank,

        -- Rank within each hour: #1 = worst supply gap at this specific hour
        row_number() over (
            partition by hour_of_day
            order by supply_gap_score desc
        )                               as gap_rank_within_hour,

        -- Rank within each station: which hour is worst for THIS station?
        row_number() over (
            partition by station_id
            order by supply_gap_score desc
        )                               as worst_hour_rank_for_station,

        -- Flag the peak gap hour for each station
        case when row_number() over (
            partition by station_id
            order by supply_gap_score desc
        ) = 1 then true else false end  as is_peak_gap_hour,

        -- Severity label for easy filtering in dashboards
        case
            when supply_gap_score >= 500  then 'CRITICAL'
            when supply_gap_score >= 200  then 'HIGH'
            when supply_gap_score >= 50   then 'MEDIUM'
            else                               'LOW'
        end                             as gap_severity

    from joined

)

select * from ranked
order by supply_gap_score desc
