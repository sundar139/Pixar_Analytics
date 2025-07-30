-- Test to ensure worldwide box office equals sum of regional box office
select *
from {{ ref('int_film_financials') }}
where abs(box_office_worldwide - (box_office_us_canada + box_office_other)) > 1000000