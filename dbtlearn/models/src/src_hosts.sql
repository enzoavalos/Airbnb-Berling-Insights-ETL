WITH raw_hosts AS (
    SELECT *
    FROM {{ ref('scd_raw_hosts') }} h
    WHERE h.DBT_VALID_TO IS NULL
)
SELECT
    id AS host_id,
    name AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts