{{ 
    config(
        unique_key='listing_month',
        on_schema_change='fail',
        incremental_strategy='delete+insert',
    )
}}

with base as (
    select 
        date_trunc('month', created_at::date) as listing_month,
        listing_id
    from {{ ref('dim_listings_cleansed') }}
),
date_bounds as (
    select 
        min(date_trunc('month', created_at::date)) as min_month,
        max(date_trunc('month', created_at::date)) as max_month
    from {{ ref('dim_listings_cleansed') }}
),
calendar_months as (
    -- Generate a monthly sequence from the first to the last month
    select dateadd(month, seq4(), min_month) as listing_month
    from table(generator(rowcount => 1200)), date_bounds
    where dateadd(month, seq4(), min_month) <= max_month
),
listings_per_month as (
    select 
        date_trunc('month', created_at::date) as listing_month,
        count(distinct listing_id) as new_listings
    from {{ ref('dim_listings_cleansed') }}
    group by 1
)
select 
    c.listing_month,
    coalesce(l.new_listings, 0) as new_listings
from calendar_months c
left join listings_per_month l
    on c.listing_month = l.listing_month
{% if is_incremental() %}
    where c.listing_month >= (
        select max(listing_month)
        from {{ this }}
    )
{% endif %}
order by c.listing_month