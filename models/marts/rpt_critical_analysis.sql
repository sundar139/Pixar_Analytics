{{ config(materialized='table') }}

with critical_metrics as (
    select
        film_title,
        release_year,
        rotten_tomatoes_score,
        metacritic_score,
        imdb_score,
        avg_critic_score,
        critical_reception,
        -- Score distributions
        case 
            when rotten_tomatoes_score >= 95 then 'Universal Acclaim'
            when rotten_tomatoes_score >= 85 then 'Critical Darling'
            when rotten_tomatoes_score >= 70 then 'Well Received'
            when rotten_tomatoes_score >= 50 then 'Mixed Reviews'
            else 'Poorly Received'
        end as rt_category,
        -- Metacritic categories
        case 
            when metacritic_score >= 90 then 'Universal Acclaim'
            when metacritic_score >= 75 then 'Generally Favorable'
            when metacritic_score >= 50 then 'Mixed Reviews'
            else 'Generally Unfavorable'
        end as metacritic_category,
        -- Score consistency check
        abs(rotten_tomatoes_score - metacritic_score) as score_variance,
        case 
            when abs(rotten_tomatoes_score - metacritic_score) <= 10 then 'Consistent'
            when abs(rotten_tomatoes_score - metacritic_score) <= 20 then 'Moderate Variance'
            else 'High Variance'
        end as score_consistency
    from {{ ref('fact_film_performance') }}
    where rotten_tomatoes_score is not null and metacritic_score is not null
),

score_rankings as (
    select
        *,
        row_number() over (order by rotten_tomatoes_score desc) as rt_rank,
        row_number() over (order by metacritic_score desc) as metacritic_rank,
        row_number() over (order by imdb_score desc) as imdb_rank,
        row_number() over (order by avg_critic_score desc) as overall_rank
    from critical_metrics
)

select * from score_rankings