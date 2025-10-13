{{ config(
    materialized='table'
) }}
with fact_reviews as (
    select * from {{ ref('fact_reviews') }}
),full_moon_listings as (
    select * from {{ ref('seed_full_moon_dates') }}
)
select
fact_reviews.*,
case
    when full_moon_listings.full_moon_date is not null then 'full_moon'
    else 'not_full_moon'
end as is_full_moon
from fact_reviews left join full_moon_listings
on (cast(fact_reviews.review_date as date) = DATEADD(DAY, 1, full_moon_listings.full_moon_date))