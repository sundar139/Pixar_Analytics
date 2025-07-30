-- Test to ensure all scores are within valid ranges
select *
from {{ ref('fact_film_performance') }}
where rotten_tomatoes_score < 0 or rotten_tomatoes_score > 100
   or metacritic_score < 0 or metacritic_score > 100
   or imdb_score < 0 or imdb_score > 10