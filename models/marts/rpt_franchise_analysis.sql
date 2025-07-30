{{ config(materialized='table') }}

with franchise_identification as (
    select
        film_title,
        case 
            when film_title like '%Toy Story%' then 'Toy Story'
            when film_title like '%Cars%' then 'Cars'
            when film_title like '%Incredibles%' then 'The Incredibles'
            when film_title like '%Monsters%' then 'Monsters'
            when film_title like '%Finding%' then 'Finding Nemo/Dory'
            else 'Standalone'
        end as franchise,
        case 
            when film_title like '%2%' or film_title like '%II%' then 2
            when film_title like '%3%' or film_title like '%III%' then 3
            when film_title like '%4%' or film_title like '%IV%' then 4
            else 1
        end as sequel_number,
        release_year,
        box_office_worldwide,
        rotten_tomatoes_score,
        budget,
        roi_ratio
    from {{ ref('fact_film_performance') }}
),

franchise_metrics as (
    select
        franchise,
        count(*) as total_films,
        sum(case when sequel_number = 1 then 1 else 0 end) as original_films,
        sum(case when sequel_number > 1 then 1 else 0 end) as sequels,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(rotten_tomatoes_score), 1) as avg_rt_score,
        round(avg(roi_ratio), 2) as avg_roi,
        sum(box_office_worldwide) as total_franchise_revenue,
        max(box_office_worldwide) as highest_grossing_entry,
        min(box_office_worldwide) as lowest_grossing_entry,
        -- Sequel performance analysis
        round(avg(case when sequel_number = 1 then box_office_worldwide end), 0) as original_avg_box_office,
        round(avg(case when sequel_number > 1 then box_office_worldwide end), 0) as sequel_avg_box_office
    from franchise_identification
    group by franchise
),

franchise_health as (
    select
        *,
        case 
            when sequel_avg_box_office > original_avg_box_office then 'Growing'
            when sequel_avg_box_office > original_avg_box_office * 0.8 then 'Stable'
            else 'Declining'
        end as franchise_trajectory,
        round((sequel_avg_box_office / nullif(original_avg_box_office, 0)) * 100, 1) as sequel_performance_pct
    from franchise_metrics
    where franchise != 'Standalone'
)

select * from franchise_health