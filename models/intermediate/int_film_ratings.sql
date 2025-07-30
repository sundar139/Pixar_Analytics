{{ config(materialized='view') }}

with film_ratings as (
    select
        f.film_title,
        f.release_date,
        p.rotten_tomatoes_score,
        p.metacritic_score,
        p.imdb_score,
        p.cinema_score,
        round((p.rotten_tomatoes_score + p.metacritic_score + (p.imdb_score * 10)) / 3, 2) as avg_critic_score,
        case 
            when p.rotten_tomatoes_score >= 90 then 'Excellent'
            when p.rotten_tomatoes_score >= 70 then 'Good'
            when p.rotten_tomatoes_score >= 50 then 'Average'
            else 'Poor'
        end as critical_reception
    from {{ ref('stg_pixar_films') }} f
    left join {{ ref('stg_public_response') }} p
        on f.film_title = p.film_title
)

select * from film_ratings