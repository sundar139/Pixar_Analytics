{{ config(materialized='view') }}

with film_financials as (
    select
        f.film_title,
        f.release_date,
        b.budget,
        b.box_office_us_canada,
        b.box_office_other,
        b.box_office_worldwide,
        b.roi_ratio,
        case 
            when b.box_office_worldwide >= b.budget * 3 then 'High Success'
            when b.box_office_worldwide >= b.budget * 2 then 'Moderate Success'
            when b.box_office_worldwide >= b.budget then 'Break Even'
            else 'Loss'
        end as financial_performance_category
    from {{ ref('stg_pixar_films') }} f
    left join {{ ref('stg_box_office') }} b
        on f.film_title = b.film_title
)

select * from film_financials