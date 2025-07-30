{{ config(materialized='table') }}

with film_awards_detail as (
    select
        d.film_key,
        d.film_title,
        a.award_category,
        a.award_status,
        a.is_winner,
        current_timestamp() as created_at
    from {{ ref('dim_films') }} d
    inner join {{ ref('stg_academy') }} a
        on d.film_title = a.film_title
)

select * from film_awards_detail