{{ config(materialized='table') }}

with laps as (
    select * from {{ ref('silver_laps') }}
),

drivers as (
    select distinct driver_number, full_name, team_name, abbreviation
    from {{ ref('silver_drivers') }}
)

select
    l.session_key,
    d.full_name,
    d.abbreviation,
    d.team_name,
    count(*)                                    as total_laps,
    round(avg(l.lap_duration_seconds), 3)       as avg_lap_seconds,
    round(min(l.lap_duration_seconds), 3)       as fastest_lap_seconds,
    round(max(l.lap_duration_seconds), 3)       as slowest_lap_seconds,
    round(avg(l.speed_trap_finish), 1)          as avg_speed_trap_kph,
    sum(cast(l.is_pit_out_lap as integer))      as pit_stops
from laps l
left join drivers d on l.driver_number = d.driver_number
group by l.session_key, d.full_name, d.abbreviation, d.team_name
order by l.session_key, avg_lap_seconds
