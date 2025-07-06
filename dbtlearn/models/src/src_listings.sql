WITH raw_listings AS (
    SELECT *
    FROM {{ ref('scd_raw_listings') }} l
    WHERE l.DBT_VALID_TO IS NULL
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM
    raw_listings