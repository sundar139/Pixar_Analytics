{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'academy') }}
),

cleaned_data as (
    select
        trim(film) as film_title,
        trim(award_type) as award_category,
        trim(status) as award_status,
        case when trim(status) = 'Won' then 1 else 0 end as is_winner,
        current_timestamp() as loaded_at
    from source_data
    where film is not null
)

select * from cleaned_data