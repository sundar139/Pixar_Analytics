{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'box_office') }}
),

cleaned_data as (
    select
        trim(film) as film_title,
        budget,
        box_office_us_canada,
        box_office_other,
        box_office_worldwide,
        case 
            when budget > 0 then round(box_office_worldwide / budget, 2)
            else null 
        end as roi_ratio,
        current_timestamp() as loaded_at
    from source_data
    where film is not null
)

select * from cleaned_data