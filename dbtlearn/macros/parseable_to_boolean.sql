{% test parseable_to_boolean(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE
  TRY_CAST({{ column_name }} AS BOOLEAN) IS NULL
    AND {{ column_name }} IS NOT NULL

{% endtest %}