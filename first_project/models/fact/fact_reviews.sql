{{ config(
    materialized='incremental',
    on_schema_change='fail'
) }}
with src_reviews as (
    select * from {{ ref('src_reviews') }}
)
select *, {{dbt_utils.default__generate_surrogate_key(['listing_id', 'reviewer_name', 'review_date','review_text'])}} as review_id
from src_reviews where review_text is not null
{% if is_incremental() %}
    {% if var("start_date",False) and var("end_date",False) %}
        {{log("Incremental run with date filter from " ~ var("start_date") ~ " to " ~ var("end_date"), info=True)}}
        and review_date >= '{{ var("start_date") }}'
        and review_date < '{{ var("end_date") }}'
        {% else %}
        and review_date > (select max(review_date) from {{ this }})
        {{log("Incremental run without date filter", info=True)}}
        {% endif %}
{% endif %}