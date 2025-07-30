{{ config(materialized='table') }}

with budget_analysis as (
    select
        film_title,
        release_year,
        budget,
        box_office_worldwide,
        roi_ratio,
        rotten_tomatoes_score,
        -- Budget categories adjusted for inflation
        case 
            when budget < 50000000 then 'Low Budget (< $50M)'
            when budget between 50000000 and 100000000 then 'Medium Budget ($50M-$100M)'
            when budget between 100000001 and 150000000 then 'High Budget ($100M-$150M)'
            else 'Ultra High Budget (> $150M)'
        end as budget_tier,
        -- Efficiency metrics
        round(box_office_worldwide / budget, 2) as revenue_multiple,
        round(rotten_tomatoes_score / (budget/1000000), 2) as quality_per_million,
        -- Market context
        avg(budget) over (partition by release_year) as year_avg_budget,
        avg(roi_ratio) over (partition by release_year) as year_avg_roi
    from {{ ref('fact_film_performance') }}
    where budget is not null and budget > 0
),

efficiency_metrics as (
    select
        budget_tier,
        count(*) as films_count,
        round(avg(budget), 0) as avg_budget,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(roi_ratio), 2) as avg_roi,
        round(avg(revenue_multiple), 2) as avg_revenue_multiple,
        round(avg(quality_per_million), 2) as avg_quality_per_million,
        -- Success rates by budget tier
        round((count(case when roi_ratio > 3.0 then 1 end) * 100.0 / count(*)), 1) as high_roi_success_rate,
        round((count(case when rotten_tomatoes_score >= 85 then 1 end) * 100.0 / count(*)), 1) as critical_success_rate
    from budget_analysis
    group by budget_tier
),

budget_evolution as (
    select
        release_year,
        round(avg(budget), 0) as yearly_avg_budget,
        round(avg(roi_ratio), 2) as yearly_avg_roi,
        count(*) as films_count,
        -- Year-over-year budget inflation
        lag(avg(budget)) over (order by release_year) as prev_year_budget,
        round(((avg(budget) - lag(avg(budget)) over (order by release_year)) / 
               lag(avg(budget)) over (order by release_year)) * 100, 1) as budget_inflation_pct
    from budget_analysis
    group by release_year
    order by release_year
)

select 
    em.*,
    be.yearly_avg_budget,
    be.budget_inflation_pct
from efficiency_metrics em
cross join (
    select avg(yearly_avg_budget) as overall_avg_budget
    from budget_evolution
) oa
left join budget_evolution be on 1=1