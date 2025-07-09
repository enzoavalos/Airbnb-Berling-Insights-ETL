with base as (
  select
    review_date,
    review_sentiment
  from {{ ref('fct_reviews') }}
),
sentiment_by_month as (
  select
    review_date,
    review_sentiment,
    date_part(month, review_date) as review_month
  from base
)
select
  review_month,
  review_sentiment,
  count(*) as reviews_count
from sentiment_by_month
group by review_month, review_sentiment
order by review_month, review_sentiment