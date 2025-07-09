{{
    config(
        materialized="view"
    )
}}

WITH base_data as (
    SELECT *
    FROM {{ ref('dim_listings_with_hosts') }}
),
enhanced_data as (
    SELECT 
        *,
        -- Create meaningful buckets for distribution analysis
        CASE 
            WHEN minimum_nights = 1 THEN '1 night'
            WHEN minimum_nights BETWEEN 2 AND 3 THEN '2-3 nights'
            WHEN minimum_nights BETWEEN 4 AND 7 THEN '4-7 nights'
            WHEN minimum_nights BETWEEN 8 AND 14 THEN '8-14 nights'
            WHEN minimum_nights BETWEEN 15 AND 30 THEN '15-30 nights'
            ELSE '30+ nights'
        END AS minimum_nights_category,
        -- Additional analytical dimensions
        CASE 
            WHEN minimum_nights <= 3 THEN 'Short-term'
            WHEN minimum_nights <= 14 THEN 'Medium-term'
            ELSE 'Long-term'
        END AS stay_duration_type,
        -- Price categorization for cross-analysis
        CASE 
            WHEN price < 50 THEN 'Budget'
            WHEN price BETWEEN 50 AND 150 THEN 'Mid-range'
            ELSE 'Premium'
        END AS price_category
    FROM base_data
)
SELECT * FROM enhanced_data