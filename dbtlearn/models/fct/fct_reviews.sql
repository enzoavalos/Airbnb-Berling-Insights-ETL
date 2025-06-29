-- on_schema_change='fail' indicates the action to take in case the underlying schema
-- for the incremental table changes, hence the existing records are invalid
{{
    config(
        materialized = 'incremental',
        on_schema_change='fail',
        incremental_strategy='delete+insert',
        unique_key='review_id'
    )
}}
WITH src_reviews AS (
    SELECT *
    FROM {{ ref('src_reviews') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }}
    AS review_id,
    *
FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
    {% if var("start_date", False) and var("end_date", False) %}
        {{ log('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ' end_date: ' ~ var("end_date") ~ ')', info=True) }}
        AND review_date >= '{{ var("start_date") }}'
        AND review_date < '{{ var("end_date") }}'
    {% else %}
        AND review_date > (select max(review_date) from {{ this }})
        {{ log('Loading ' ~ this ~ ' incrementally (all missing dates)', info=True) }}
    {% endif %}
{% endif %}

-- Only load those records with a later date than the last one saved, or define a date range to reload incrementally.
-- In the latter an incremental strategy must be applied to ensure uniqueness via merging, overriding, and so on.