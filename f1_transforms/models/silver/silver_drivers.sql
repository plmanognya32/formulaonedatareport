{{ config(materialized='table') }}

with raw_drivers as (
    select *
    from read_json_auto(
        '/workspaces/formulaonedatareport/data/bronze/drivers/2024/*.json',
        format='array'
    )
)

select distinct
    cast(driver_number as integer) as driver_number,
    cast(session_key as integer)   as session_key,
    full_name,
    name_acronym                   as abbreviation,
    team_name,
    team_colour,
    country_code,
    headshot_url
from raw_drivers
where full_name is not null
