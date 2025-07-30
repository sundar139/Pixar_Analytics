{{ config(materialized='table') }}

with genre_split as (
    select
        f.film_title,
        f.release_year,
        f.box_office_worldwide,
        f.rotten_tomatoes_score,
        f.budget,
        f.roi_ratio,
        trim(g.genre_value) as genre
    from {{ ref('fact_film_performance') }} f
    inner join {{ ref('stg_genre') }} g on f.film_title = g.film_title
    where g.genre_category = 'Genre'
),

genre_metrics as (
    select
        genre,
        count(*) as films_count,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(budget), 0) as avg_budget,
        round(avg(roi_ratio), 2) as avg_roi,
        round(avg(rotten_tomatoes_score), 1) as avg_rt_score,
        max(box_office_worldwide) as highest_grossing,
        min(box_office_worldwide) as lowest_grossing,
        -- Calculate success rate (films with ROI > 2.0)
        round((count(case when roi_ratio > 2.0 then 1 end) * 100.0 / count(*)), 1) as success_rate_pct
    from genre_split
    where box_office_worldwide is not null 
    and budget is not null
    group by genre
),

genre_rankings as (
    select
        *,
        row_number() over (order by avg_box_office desc) as box_office_rank,
        row_number() over (order by avg_roi desc) as roi_rank,
        row_number() over (order by avg_rt_score desc) as critical_rank,
        row_number() over (order by success_rate_pct desc) as success_rank
    from genre_metrics
)

select * from genre_rankings