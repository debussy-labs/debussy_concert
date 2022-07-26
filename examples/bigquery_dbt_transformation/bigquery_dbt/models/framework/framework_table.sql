{{ config(materialized='table') }}

with source_data as (

    select 'debussy_concert' as framework
    union all
    select null as framework

)

select *
from source_data
