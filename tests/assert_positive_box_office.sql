-- Test to ensure all box office values are positive
select *
from {{ ref('fact_film_performance') }}
where box_office_worldwide < 0