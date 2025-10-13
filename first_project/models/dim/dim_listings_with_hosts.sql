WITH 
listings AS (
    select * from {{ ref('dim_listings_cleansed') }}
),
hosts AS (
    select * from {{ ref('dim_hosts_cleansed') }}
)

SELECT 
    listings.listing_id,
    listings.listing_name,
    listings.listing_url,
    listings.room_type,
    listings.minimum_nights,
    listings.host_id,
    listings.price,
    listings.created_at,
    hosts.host_name,
    hosts.is_superhost as host_is_superhost,
    greatest(listings.updated_at, hosts.updated_at) as updated_at
FROM listings
left join hosts on listings.host_id = hosts.host_id