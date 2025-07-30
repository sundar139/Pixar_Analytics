{{ config(materialized='table') }}

with financial_metrics as (
    select
        film_title,
        release_year,
        budget,
        box_office_worldwide,
        roi_ratio,
        financial_performance_category,
        -- Calculate percentile rankings
        percent_rank() over (order by box_office_worldwide) as box_office_percentile,
        percent_rank() over (order by roi_ratio) as roi_percentile,
        -- Calculate decade groupings
        case 
            when release_year between 1995 and 1999 then '1990s'
            when release_year between 2000 and 2009 then '2000s'
            when release_year between 2010 and 2019 then '2010s'
            when release_year >= 2020 then '2020s'
        end as decade,
        -- Market performance indicators
        case 
            when box_office_worldwide >= 1000000000 then 'Billion Dollar Club'
            when box_office_worldwide >= 500000000 then 'Blockbuster'
            when box_office_worldwide >= 200000000 then 'Hit'
            else 'Moderate'
        end as market_tier
    from {{ ref('fact_film_performance') }}
    where budget is not null and box_office_worldwide is not null
),

decade_stats as (
    select
        decade,
        count(*) as films_count,
        round(avg(budget), 0) as avg_budget,
        round(avg(box_office_worldwide), 0) as avg_box_office,
        round(avg(roi_ratio), 2) as avg_roi,
        max(box_office_worldwide) as highest_grossing,
        min(box_office_worldwide) as lowest_grossing
    from financial_metrics
    group by decade
),

final as (
    select
        f.*,
        d.avg_budget as decade_avg_budget,
        d.avg_box_office as decade_avg_box_office,
        d.avg_roi as decade_avg_roi,
        -- Performance vs decade average
        case 
            when f.box_office_worldwide > d.avg_box_office then 'Above Average'
            else 'Below Average'
        end as vs_decade_performance
    from financial_metrics f
    left join decade_stats d on f.decade = d.decade
)

select * from final