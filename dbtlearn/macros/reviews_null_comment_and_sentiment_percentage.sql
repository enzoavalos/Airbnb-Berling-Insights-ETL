{% test reviews_null_comment_and_sentiment_percentage(model, percent) %}

WITH total AS (
    SELECT COUNT(*) AS total_count
    FROM {{ model }}
),
zero AS (
    SELECT COUNT(*) AS zero_count
    FROM {{ model }}
    WHERE review_text IS NULL
    AND review_sentiment IS NULL
)
SELECT *
FROM total, zero
WHERE zero_count > ({{ percent }} / 100) * total_count

{% endtest %}