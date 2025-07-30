{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'pixar_people') }}
),

cleaned_data as (
    select
        trim(film) as film_title,
        trim(role_type) as role_type,
        trim(name) as person_name,
        current_timestamp() as loaded_at
    from source_data
    where film is not null 
    and role_type is not null 
    and name is not null
)

select * from cleaned_data