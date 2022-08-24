with city as (
    SELECT city_id, city, country_id
    FROM {{ source('sakila', 'mssql_sakila_city') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY _ts_ingestion DESC) = 1
),
country as (
    SELECT country_id, country
    FROM {{ source('sakila', 'mssql_sakila_country') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY _ts_ingestion DESC) = 1
),
denormalized as (
select city.city_id, city.city, country.country,
    TIMESTAMP("{{ var('execution_date') }}") as _airflow_execution_date,
    "{{ env_var('DEBUSSY_CONCERT__PROJECT') }}" as _project
from city
join country on city.country_id = country.country_id
)

select * from denormalized

