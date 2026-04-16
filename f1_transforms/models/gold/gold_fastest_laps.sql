{{ config(materialized='table') }}

with laps as (
    select * from {{ ref('silver_laps') }}
),

drivers as (
    select distinct driver_number, full_name, team_name, abbreviation
    from {{ ref('silver_drivers') }}
),

ranked as (
    select
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_duration_seconds,
        l.sector_1_seconds,
        l.sector_2_seconds,
        l.sector_3_seconds,
        l.speed_trap_finish,
        row_number() over (
            partition by l.session_key, l.driver_number
            order by l.lap_duration_seconds asc
        ) as rn
    from laps l
    where l.lap_duration_seconds is not null
)

select
    r.session_key,
    d.full_name,
    d.abbreviation,
    d.team_name,
    r.lap_number          as fastest_lap_number,
    round(r.lap_duration_seconds, 3)  as fastest_lap_seconds,
    round(r.sector_1_seconds, 3)      as best_s1,
    round(r.sector_2_seconds, 3)      as best_s2,
    round(r.sector_3_seconds, 3)      as best_s3,
    r.speed_trap_finish
from ranked r
left join drivers d on r.driver_number = d.driver_number
where r.rn = 1
order by r.session_key, r.lap_duration_seconds
