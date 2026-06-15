-- =============================================================================
--  int_station_hourly_demand.sql  (Intermediate Model)
-- =============================================================================
--  PURPOSE:
--    Aggregate trip history by station and hour of day to compute
--    HISTORICAL DEMAND metrics.
--
--    This answers the "demand side" of our business question:
--    "Which stations have the HIGHEST historical trip demand?"
--
--  LOGIC:
--    For each station, for each hour of the day (0-23), count:
--      - Total trips that STARTED here (departures = bikes taken out)
--      - Total trips that ENDED here (arrivals = bikes returned)
--      - Net demand = departures - arrivals
--        Positive net demand = bikes leave faster than they arrive = shortage zone
--
--  OUTPUT:
--    One row per (station_id, start_hour) with aggregated demand stats
--    covering all available historical data.
-- =============================================================================

with trips as (

    select * from {{ ref('stg_trip_history') }}

),

-- Count trips DEPARTING from each station, by hour of day
departures as (

    select
        start_station_id                            as station_id,
        start_station_name                          as station_name,
        start_hour                                  as hour_of_day,
        start_day_of_week,
        start_day_name,

        count(*)                                    as total_departures,
        avg(trip_duration_minutes)                  as avg_trip_duration_minutes,
        countif(member_casual = 'member')           as member_departures,
        countif(member_casual = 'casual')           as casual_departures,
        countif(rideable_type = 'electric_bike')    as ebike_departures

    from trips
    group by 1, 2, 3, 4, 5

),

-- Count trips ARRIVING at each station, by hour of day
arrivals as (

    select
        end_station_id                              as station_id,
        start_hour                                  as hour_of_day,   -- hour trip ended
        start_day_of_week,

        count(*)                                    as total_arrivals

    from trips
    group by 1, 2, 3

),

-- Join departures and arrivals to compute net demand
combined as (

    select
        d.station_id,
        d.station_name,
        d.hour_of_day,
        d.start_day_of_week,
        d.start_day_name,

        -- Core demand metrics
        d.total_departures,
        coalesce(a.total_arrivals, 0)               as total_arrivals,

        -- Net demand: how many more bikes leave than arrive
        -- High positive value = chronic shortage at this hour
        d.total_departures - coalesce(a.total_arrivals, 0)  as net_demand,

        -- Demand ratio: departures / arrivals (>1 means demand > supply)
        safe_divide(
            d.total_departures,
            nullif(coalesce(a.total_arrivals, 0), 0)
        )                                           as demand_to_arrival_ratio,

        d.avg_trip_duration_minutes,
        d.member_departures,
        d.casual_departures,
        d.ebike_departures,

        -- Rank within each hour (1 = busiest station at that hour)
        row_number() over (
            partition by d.hour_of_day
            order by d.total_departures desc
        )                                           as rank_by_departures_in_hour

    from departures d
    left join arrivals a
        on d.station_id     = a.station_id
        and d.hour_of_day   = a.hour_of_day
        and d.start_day_of_week = a.start_day_of_week

)

select * from combined
