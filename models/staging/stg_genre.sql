{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'genre') }}
),

cleaned_data as (
    select
        trim(film) as film_title,
        trim(category) as genre_category,
        trim(value) as genre_value,
        current_timestamp() as loaded_at
    from source_data
    where film is not null
)

select * from cleaned_data