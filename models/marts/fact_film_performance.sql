{{ config(materialized='table') }}

with film_performance as (
    select
        d.film_key,
        d.film_title,
        d.release_date,
        d.release_year,
        d.runtime_minutes,
        d.mpaa_rating,
        f.budget,
        f.box_office_worldwide,
        f.roi_ratio,
        f.financial_performance_category,
        r.rotten_tomatoes_score,
        r.metacritic_score,
        r.imdb_score,
        r.avg_critic_score,
        r.critical_reception,
        a.total_nominations,
        a.total_wins,
        a.major_nominations,
        a.major_wins,
        current_timestamp() as created_at
    from {{ ref('dim_films') }} d
    left join {{ ref('int_film_financials') }} f
        on d.film_title = f.film_title
    left join {{ ref('int_film_ratings') }} r
        on d.film_title = r.film_title
    left join {{ ref('int_film_awards') }} a
        on d.film_title = a.film_title
)

select * from film_performance