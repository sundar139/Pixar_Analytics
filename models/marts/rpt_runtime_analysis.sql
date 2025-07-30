{{ config(materialized='table') }}

with runtime_analysis as (
    select
        film_title,
        release_year,
        runtime_minutes,
        box_office_worldwide,
        rotten_tomatoes_score,
        -- Runtime categories
        case 
            when runtime_minutes < 90 then 'Short (< 90 min)'
            when runtime_minutes between 90 and 105 then 'Standard (90-105 min)'
            when runtime_minutes between 106 and 120 then 'Extended (106-120 min)'
            else 'Long (> 120 min)'
        end as runtime_category,
        -- Industry evolution context
        avg(runtime_minutes) over (
            order by release_year 
            rows between 2 preceding and 2 following
        ) as rolling_avg_runtime
    from {{ ref('fact_film_performance') }}
    where runtime_minutes is not null
),

runtime_performance as (
    select
        runtime_category,
        count(*) as films_count,
        round(avg(runtime_minutes), 1) as avg_runtime,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(rotten_tomatoes_score), 1) as avg_rt_score,
        -- Audience engagement metrics
        round(avg(box_office_worldwide / runtime_minutes), 0) as revenue_per_minute,
        round((count(case when rotten_tomatoes_score >= 80 then 1 end) * 100.0 / count(*)), 1) as critical_success_rate
    from runtime_analysis
    group by runtime_category
),

runtime_trends as (
    select
        release_year,
        round(avg(runtime_minutes), 1) as yearly_avg_runtime,
        round(avg(rolling_avg_runtime), 1) as trend_runtime,
        count(*) as films_that_year
    from runtime_analysis
    group by release_year
    order by release_year
)

select 
    rp.*,
    rt.yearly_avg_runtime,
    rt.trend_runtime
from runtime_performance rp
cross join (
    select avg(yearly_avg_runtime) as overall_avg_runtime
    from runtime_trends
) oa
left join runtime_trends rt on 1=1  -- This creates a cross join for reference