with reviews_fullmoon as (
  select
    r.listing_id,
    r.review_sentiment,
    l.price
  from {{ ref('mart_fullmoon_reviews') }} r
  join {{ ref('dim_listings_cleansed') }} l using (listing_id)
  where r.is_full_moon = TRUE
)
select
  review_sentiment,
  count(*) as review_count,
  round(avg(price), 2) as avg_price,
  round(stddev(price), 2) as price_stddev,
  round(percentile_cont(0.5) within group(order by price), 2) as median_price,
  min(price) as min_price,
  max(price) as max_price
from reviews_fullmoon
group by review_sentiment
order by avg_price desc