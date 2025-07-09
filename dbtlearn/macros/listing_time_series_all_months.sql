-- This test ensures that all months between the earliest and latest month with recorded new listings are included in the time series

{% test listing_time_series_all_months(model, column_name) %}

with date_bounds as (
    select 
        min(date_trunc('month', created_at::date)) as min_month,
        max(date_trunc('month', created_at::date)) as max_month
    from {{ ref('dim_listings_cleansed') }}
),
expected_months as (
    select dateadd(month, seq4(), min_month) as listing_month
    from table(generator(rowcount => 1200)), date_bounds
    where dateadd(month, seq4(), min_month) <= (select max_month from date_bounds)
),
actual_months as (
    select distinct {{ column_name }} as listing_month
    from {{ ref('new_listings_time_series') }}
),
missing_months as (
    select e.listing_month
    from expected_months e
    left join actual_months a on a.listing_month = e.listing_month
    where a.listing_month is null
)
select listing_month
from missing_months

{% endtest %}