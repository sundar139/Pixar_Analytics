{{ config(materialized='table') }}

with market_context as (
    select
        film_title,
        release_date,
        release_year,
        box_office_worldwide,
        rotten_tomatoes_score,
        budget,
        -- Create market performance benchmarks
        avg(box_office_worldwide) over (partition by release_year) as year_avg_box_office,
        max(box_office_worldwide) over (partition by release_year) as year_max_box_office,
        count(*) over (partition by release_year) as films_that_year,
        -- Decade benchmarks
        avg(box_office_worldwide) over (
            partition by floor(release_year/10)*10
        ) as decade_avg_box_office
    from {{ ref('fact_film_performance') }}
),

competitive_metrics as (
    select
        *,
        -- Market share within release year
        round((box_office_worldwide / year_avg_box_office) * 100, 1) as vs_year_avg_pct,
        round((box_office_worldwide / year_max_box_office) * 100, 1) as vs_year_best_pct,
        -- Performance categories
        case 
            when box_office_worldwide = year_max_box_office then 'Year Leader'
            when box_office_worldwide > year_avg_box_office * 1.5 then 'Above Market'
            when box_office_worldwide > year_avg_box_office then 'Market Performer'
            else 'Below Market'
        end as market_position,
        -- Decade comparison
        case 
            when box_office_worldwide > decade_avg_box_office * 1.2 then 'Decade Outperformer'
            when box_office_worldwide > decade_avg_box_office * 0.8 then 'Decade Average'
            else 'Decade Underperformer'
        end as decade_position
    from market_context
)

select * from competitive_metrics