{{
    config(
        materialized='incremental',
        unique_key='profile_run_id',
        merge_exclude_columns=['profiled_at'],
        on_schema_change='sync_all_columns',
        cluster_by=['model_name', 'profile_date'],
        tags=['data_quality', 'profiling']
    )
}}

-- depends_on: {{ ref('mart_fullmoon_reviews') }}

{{ 
    profile_model_incremental(
        model_name='mart_fullmoon_reviews',
        date_column='review_date',
        profile_frequency='daily',
        include_advanced_stats=true
    )
}}

{% if is_incremental() %}
    where profile_run_id not in (
        select distinct profile_run_id 
        from {{ this }}
    )
{% endif %}