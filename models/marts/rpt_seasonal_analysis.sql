{{ config(materialized='table') }}

with seasonal_data as (
    select
        film_title,
        release_date,
        extract(month from release_date) as release_month,
        case 
            when extract(month from release_date) in (3, 4, 5) then 'Spring'
            when extract(month from release_date) in (6, 7, 8) then 'Summer'
            when extract(month from release_date) in (9, 10, 11) then 'Fall'
            when extract(month from release_date) in (12, 1, 2) then 'Winter'
        end as release_season,
        case 
            when extract(month from release_date) in (6, 7) then 'Peak Summer'
            when extract(month from release_date) in (11, 12) then 'Holiday Season'
            when extract(month from release_date) in (3, 4) then 'Spring Break'
            else 'Off-Season'
        end as market_window,
        box_office_worldwide,
        rotten_tomatoes_score,
        budget,
        roi_ratio
    from {{ ref('fact_film_performance') }}
),

seasonal_performance as (
    select
        release_season,
        market_window,
        count(*) as films_released,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(rotten_tomatoes_score), 1) as avg_rt_score,
        round(avg(roi_ratio), 2) as avg_roi,
        max(box_office_worldwide) as season_high,
        min(box_office_worldwide) as season_low,
        -- Success rate analysis
        round((count(case when roi_ratio > 2.5 then 1 end) * 100.0 / count(*)), 1) as high_success_rate_pct
    from seasonal_data
    group by release_season, market_window
),

seasonal_rankings as (
    select
        *,
        row_number() over (order by avg_box_office desc) as box_office_rank,
        row_number() over (order by avg_roi desc) as roi_rank,
        row_number() over (order by high_success_rate_pct desc) as success_rank
    from seasonal_performance
)

select * from seasonal_rankings