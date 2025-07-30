{{ config(materialized='table') }}

with director_performance as (
    select
        p.person_name as director_name,
        count(distinct f.film_title) as films_directed,
        round(avg(f.box_office_worldwide), 0) as avg_box_office,
        round(avg(f.rotten_tomatoes_score), 1) as avg_rt_score,
        round(avg(f.roi_ratio), 2) as avg_roi,
        sum(f.total_wins) as total_oscar_wins,
        sum(f.total_nominations) as total_oscar_nominations,
        max(f.box_office_worldwide) as highest_grossing_film,
        min(f.release_date) as first_film_date,
        max(f.release_date) as latest_film_date
    from {{ ref('stg_pixar_people') }} p
    inner join {{ ref('fact_film_performance') }} f on p.film_title = f.film_title
    where p.role_type = 'Director'
    group by p.person_name
),

producer_performance as (
    select
        p.person_name as producer_name,
        count(distinct f.film_title) as films_produced,
        round(avg(f.box_office_worldwide), 0) as avg_box_office,
        round(avg(f.rotten_tomatoes_score), 1) as avg_rt_score,
        sum(f.total_wins) as total_oscar_wins,
        sum(f.total_nominations) as total_oscar_nominations
    from {{ ref('stg_pixar_people') }} p
    inner join {{ ref('fact_film_performance') }} f on p.film_title = f.film_title
    where p.role_type = 'Producer'
    group by p.person_name
),

director_rankings as (
    select
        *,
        row_number() over (order by avg_box_office desc) as box_office_rank,
        row_number() over (order by avg_rt_score desc) as critical_rank,
        row_number() over (order by total_oscar_wins desc) as awards_rank,
        -- Career span calculation
        datediff('year', first_film_date, latest_film_date) as career_span_years
    from director_performance
    where films_directed >= 1
)

select * from director_rankings