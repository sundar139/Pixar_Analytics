{{ config(materialized='table') }}

with yearly_trends as (
    select
        release_year,
        count(*) as films_released,
        round(avg(budget), 0) as avg_budget,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(runtime_minutes), 1) as avg_runtime,
        round(avg(rotten_tomatoes_score), 1) as avg_rt_score,
        round(avg(metacritic_score), 1) as avg_metacritic_score,
        round(avg(imdb_score), 1) as avg_imdb_score,
        sum(total_nominations) as total_oscar_noms,
        sum(total_wins) as total_oscar_wins
    from {{ ref('fact_film_performance') }}
    group by release_year
),

trend_analysis as (
    select
        *,
        -- Calculate year-over-year changes
        lag(avg_budget) over (order by release_year) as prev_year_budget,
        lag(avg_box_office) over (order by release_year) as prev_year_box_office,
        lag(avg_rt_score) over (order by release_year) as prev_year_rt_score,
        -- Calculate moving averages (3-year window)
        round(avg(avg_budget) over (order by release_year rows between 2 preceding and current row), 0) as budget_3yr_avg,
        round(avg(avg_box_office) over (order by release_year rows between 2 preceding and current row), 0) as box_office_3yr_avg,
        round(avg(avg_rt_score) over (order by release_year rows between 2 preceding and current row), 1) as rt_score_3yr_avg
    from yearly_trends
),

final_trends as (
    select
        *,
        -- Calculate percentage changes
        case 
            when prev_year_budget > 0 then 
                round(((avg_budget - prev_year_budget) / prev_year_budget) * 100, 1)
            else null 
        end as budget_yoy_change_pct,
        case 
            when prev_year_box_office > 0 then 
                round(((avg_box_office - prev_year_box_office) / prev_year_box_office) * 100, 1)
            else null 
        end as box_office_yoy_change_pct,
        case 
            when prev_year_rt_score > 0 then 
                round(avg_rt_score - prev_year_rt_score, 1)
            else null 
        end as rt_score_yoy_change
    from trend_analysis
)

select * from final_trends