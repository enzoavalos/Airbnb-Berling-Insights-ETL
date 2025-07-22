{{
    config(
        tags=['data_quality', 'profiling']
    )
}}

-- This are the deepest models in the DAG
-- depends_on: {{ ref('dim_listings_with_hosts_distribution_analysis') }}
-- depends_on: {{ ref('price_sentiment_correlation') }}

{{ analyze_downstream_impact() }}