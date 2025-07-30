{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'pixar_films') }}
),

cleaned_data as (
    select
        number as film_sequence_number,
        trim(film) as film_title,
        to_date(release_date, 'YYYY-MM-DD') as release_date,
        run_time as runtime_minutes,
        trim(film_rating) as mpaa_rating,
        trim(plot) as plot_summary,
        current_timestamp() as loaded_at
    from source_data
)

select * from cleaned_data