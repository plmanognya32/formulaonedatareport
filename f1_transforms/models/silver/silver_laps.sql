{{ config(materialized='table') }}

with raw_laps as (
    select *
    from read_json_auto(
        '/workspaces/formulaonedatareport/data/bronze/laps/2024/*.json',
        format='array'
    )
)

select
    cast(session_key as integer)       as session_key,
    cast(driver_number as integer)     as driver_number,
    cast(lap_number as integer)        as lap_number,
    cast(lap_duration as float)        as lap_duration_seconds,
    cast(duration_sector_1 as float)   as sector_1_seconds,
    cast(duration_sector_2 as float)   as sector_2_seconds,
    cast(duration_sector_3 as float)   as sector_3_seconds,
    cast(i1_speed as integer)          as speed_trap_1,
    cast(i2_speed as integer)          as speed_trap_2,
    cast(st_speed as integer)          as speed_trap_finish,
    cast(is_pit_out_lap as boolean)    as is_pit_out_lap,
    date_trunc('second', cast(date_start as timestamp)) as lap_start_time
from raw_laps
where lap_duration is not null
  and lap_duration > 60
