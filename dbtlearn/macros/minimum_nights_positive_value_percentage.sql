-- This test fails if over 5% of records have a value of 0 as their declared minimum nights
{% test minimum_nights_positive_value_percentage(model, column_name, percent) %}

WITH total AS (
    SELECT COUNT(*) AS total_count
    FROM {{ model }}
),
zero AS (
    SELECT COUNT(*) AS zero_count
    FROM {{ model }}
    WHERE {{ column_name }} = 0
)
SELECT *
FROM total, zero
WHERE zero_count > ({{ percent }} / 100) * total_count

{% endtest %}