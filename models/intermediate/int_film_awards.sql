{{ config(materialized='view') }}

with film_awards as (
    select
        film_title,
        count(*) as total_nominations,
        sum(is_winner) as total_wins,
        count(case when award_category like '%Best%' then 1 end) as major_nominations,
        sum(case when award_category like '%Best%' and is_winner = 1 then 1 end) as major_wins
    from {{ ref('stg_academy') }}
    group by film_title
)

select * from film_awards