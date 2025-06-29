{% test price_str_parseable_to_positive_number(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE
  TRY_CAST(REPLACE({{ column_name }}, '$', '') AS FLOAT) IS NULL
  OR
  TRY_CAST(REPLACE({{ column_name }}, '$', '') AS FLOAT) < 0

{% endtest %}