{{ config(materialized='table') }}

with people_roles as (
    select
        {{ dbt_utils.generate_surrogate_key(['person_name']) }} as person_key,
        person_name,
        listagg(distinct role_type, ', ') within group (order by role_type) as roles,
        count(distinct film_title) as films_count,
        min(loaded_at) as first_loaded_at,
        max(loaded_at) as last_loaded_at
    from {{ ref('stg_pixar_people') }}
    group by person_name
)

select * from people_roles