{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'public_response') }}
),

cleaned_data as (
    select
        trim(film) as film_title,
        rotten_tomatoes_score,
        rotten_tomatoes_counts,
        metacritic_score,
        metacritic_counts,
        trim(cinema_score) as cinema_score,
        imdb_score,
        imdb_counts,
        current_timestamp() as loaded_at
    from source_data
    where film is not null
)

select * from cleaned_data