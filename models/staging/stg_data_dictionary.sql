{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('raw_pixar', 'pixar_data_dictionary') }}
),

cleaned_data as (
    select
        trim(TABLE_NAME) as table_name,
        trim(FIELD) as field_name,
        trim(DESCRIPTION) as field_description,
        current_timestamp() as loaded_at
    from source_data
)

select * from cleaned_data