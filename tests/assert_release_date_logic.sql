-- Test to ensure release dates are logical
select *
from {{ ref('dim_films') }}
where release_date < '1995-01-01' or release_date > current_date()