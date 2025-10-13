{{ config(
    materialized='view'
) }}
with src_listings as (
    select * from {{ ref('src_listings') }}
)
select 
    listing_id,
    listing_name,
    listing_url,
    room_type,
    case 
        when minimum_nights = 0 then 1
        else minimum_nights
    end as minimum_nights,
    host_id,
    cast(replace(price_str, '$', '') as float) as price,
    created_at,
    updated_at
from src_listings