{{ config(materialized='table') }}

with film_base as (
    select
        {{ dbt_utils.generate_surrogate_key(['film_title']) }} as film_key,
        film_sequence_number,
        film_title,
        release_date,
        extract(year from release_date) as release_year,
        runtime_minutes,
        mpaa_rating,
        plot_summary,
        loaded_at
    from {{ ref('stg_pixar_films') }}
),

film_genres as (
    select
        film_title,
        listagg(genre_value, ', ') within group (order by genre_value) as genres
    from {{ ref('stg_genre') }}
    group by film_title
),

final as (
    select
        f.film_key,
        f.film_sequence_number,
        f.film_title,
        f.release_date,
        f.release_year,
        f.runtime_minutes,
        f.mpaa_rating,
        f.plot_summary,
        g.genres,
        f.loaded_at
    from film_base f
    left join film_genres g
        on f.film_title = g.film_title
)

select * from final