{{ config(materialized='table') }}

with awards_summary as (
    select
        f.film_title,
        f.release_year,
        f.box_office_worldwide,
        f.rotten_tomatoes_score,
        a.award_category,
        a.award_status,
        a.is_winner,
        -- Categorize award types
        case 
            when a.award_category like '%Best Animated%' then 'Best Animated Feature'
            when a.award_category like '%Best Original Song%' then 'Best Original Song'
            when a.award_category like '%Best Original Score%' then 'Best Original Score'
            when a.award_category like '%Best Sound%' then 'Technical Awards'
            else 'Other'
        end as award_type_category
    from {{ ref('fact_film_performance') }} f
    inner join {{ ref('stg_academy') }} a on f.film_title = a.film_title
),

film_awards_summary as (
    select
        film_title,
        release_year,
        box_office_worldwide,
        rotten_tomatoes_score,
        count(*) as total_nominations,
        sum(is_winner) as total_wins,
        -- Calculate win rate
        round((sum(is_winner) * 100.0 / count(*)), 1) as win_rate_pct,
        -- Check for major categories
        max(case when award_type_category = 'Best Animated Feature' and is_winner = 1 then 1 else 0 end) as won_best_animated,
        max(case when award_type_category = 'Best Original Song' and is_winner = 1 then 1 else 0 end) as won_best_song,
        max(case when award_type_category = 'Best Original Score' and is_winner = 1 then 1 else 0 end) as won_best_score
    from awards_summary
    group by film_title, release_year, box_office_worldwide, rotten_tomatoes_score
),

awards_correlation as (
    select
        *,
        -- Analyze correlation between awards and performance
        case 
            when total_wins >= 2 then 'Multiple Winner'
            when total_wins = 1 then 'Single Winner'
            when total_nominations >= 2 then 'Multiple Nominee'
            else 'Single Nominee'
        end as awards_tier,
        -- Awards vs Box Office correlation
        case 
            when total_wins > 0 and box_office_worldwide > 500000000 then 'Award Winner + Blockbuster'
            when total_wins > 0 then 'Award Winner'
            when box_office_worldwide > 500000000 then 'Blockbuster'
            else 'Standard'
        end as success_category
    from film_awards_summary
)

select * from awards_correlation