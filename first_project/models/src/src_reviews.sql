with src_reviews as (
    select * from {{ source('airbnb', 'reviews') }}
)
select
    listing_id,
    reviewer_name,
    comments as review_text,
    date as review_date,
    sentiment as review_sentiment
from src_reviews