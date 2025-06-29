{% test is_superhost_null_percentage(model, column_name, percent) %}

WITH total AS (
    SELECT COUNT(*) AS total_count
    FROM {{ model }}
),
zero AS (
    SELECT COUNT(*) AS zero_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
)
SELECT *
FROM total, zero
WHERE zero_count > ({{ percent }} / 100) * total_count

{% endtest %}