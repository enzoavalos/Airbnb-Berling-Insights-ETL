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

-- depends_on: {{ ref('dim_listings_with_hosts') }}

{{ 
    profile_model_incremental(
        model_name='dim_listings_with_hosts',
        date_column='updated_at',
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