-- This test verifies that theres only ONE active valid row for each ID in a SCD Type 2 snapshot

{% test assert_single_active_version(model, column_name) %}

SELECT
  {{ column_name }} as offending_id,
  count(*) as active_versions
FROM {{ model }}
WHERE dbt_valid_to IS NULL
GROUP BY {{ column_name }}
HAVING count(*) > 1

{% endtest %}