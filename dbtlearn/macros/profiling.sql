-- Advanced DBT Profiling Macro with comprehensive metadata collection

{% macro profile_model_incremental(
    model_name,
    sample_size=null,
    include_advanced_stats=true,
    exclude_columns=[],
    data_freshness_threshold_hours=24,
    date_column=null,
    profile_frequency='daily') %}
    
    {% if execute %}
        -- Input validation and error handling
        {% if not model_name %}
            {{ exceptions.raise_compiler_error("model_name is required for profiling") }}
        {% endif %}

        -- Get model relation and validate it exists
        {% set rel = ref(model_name) %}

        {% if not rel %}
            {{ exceptions.raise_compiler_error("Model '" ~ model_name ~ "' not found") }}
        {% endif %}

        -- Get columns and filter out excluded ones
        {% set all_cols = adapter.get_columns_in_relation(rel) %}
        {% set cols = [] %}

        {% for col in all_cols %}
            {% if col.name.lower() not in (exclude_columns | map('lower') | list) %}
                {% set _ = cols.append(col) %}
            {% endif %}
        {% endfor %}

        -- Build sampling clause if specified
        {% set sample_clause = "" %}
        {% if sample_size %}
            {% if target.type == 'snowflake' %}
                {% set sample_clause = "sample bernoulli (" ~ sample_size ~ ")" %}
            {% endif %}
        {% endif %}

        -- Generate profile date based on frequency
        {% set profile_date_expr %}
            {% if profile_frequency == 'hourly' %}
                date_trunc('hour', current_timestamp())
            {% elif profile_frequency == 'weekly' %}
                date_trunc('week', current_timestamp())
            {% elif profile_frequency == 'monthly' %}
                date_trunc('month', current_timestamp())
            {% else %}
                date_trunc('day', current_timestamp())
            {% endif %}
        {% endset %}

        {{ log("Profiling model incrementally: " ~ model_name ~ " (frequency: " ~ profile_frequency ~ ")", info=True) }}

        -- Main profiling query with incremental logic
        with base_data as (
            select *
            from {{ rel }}
            {% if sample_clause %}
                {{ sample_clause }}
            {% endif %}
        ),
        -- Basic row-level metrics
        row_metrics as (
            select
                count(*) as total_rows,
                count(distinct concat(
                {% for col in cols %}
                    coalesce(cast({{ col.name }} as string), 'NULL')
                    {%- if not loop.last -%},{%- endif -%}
                {% endfor %}
                )) as distinct_rows
            from base_data
        ),
        -- Column-level profiling with enhanced metrics
        column_metrics as (
            select
                {% for col in cols %}
                    -- Boolean column metrics
                    {% if col.data_type.upper() in ('BOOLEAN','BOOL') %}
                        sum(case when {{ col.name }} = true then 1 else 0 end) as {{ col.name }}_true_count,
                        sum(case when {{ col.name }} = false then 1 else 0 end) as {{ col.name }}_false_count,
                    
                    -- Date/Timestamp column metrics
                    {% elif col.data_type.upper() in ('DATE','TIMESTAMP','DATETIME','TIME','TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ') %}
                        min({{ col.name }}) as {{ col.name }}_earliest,
                        max({{ col.name }}) as {{ col.name }}_latest,
                        {% if include_advanced_stats %}
                            -- Date range analysis
                            datediff(day, min({{ col.name }}), max({{ col.name }})) as {{ col.name }}_date_range_days,
                            -- Count of distinct dates
                            count(distinct date({{ col.name }})) as {{ col.name }}_distinct_dates,
                            -- Records per day average
                            round(count(*) / nullif(count(distinct date({{ col.name }})), 0), 2) as {{ col.name }}_records_per_day_avg,
                        {% endif %}
                    
                    -- String column metrics
                    {% elif col.data_type.upper() in ('STRING','TEXT','VARCHAR','CHAR','NVARCHAR','NCHAR') %}
                        min(length({{ col.name }})) as {{ col.name }}_min_length,
                        max(length({{ col.name }})) as {{ col.name }}_max_length,
                        avg(length({{ col.name }})) as {{ col.name }}_avg_length,
                        {% if include_advanced_stats %}
                            -- Most frequent value
                            mode({{ col.name }}) as {{ col.name }}_mode,
                            -- Empty string count
                            sum(case when {{ col.name }} = '' then 1 else 0 end) as {{ col.name }}_empty_count,
                            -- Whitespace-only count
                            sum(case when trim({{ col.name }}) = '' and {{ col.name }} != '' then 1 else 0 end) as {{ col.name }}_whitespace_only_count,
                            -- Pattern analysis
                            sum(case when regexp_like({{ col.name }}, '^[0-9]+$') then 1 else 0 end) as {{ col.name }}_numeric_pattern_count,
                            sum(case when regexp_like({{ col.name }}, '^[A-Za-z\\s]+$') then 1 else 0 end) as {{ col.name }}_alpha_pattern_count,
                        {% endif %}
                    
                    -- Numeric column metrics
                    {% elif col.data_type.upper() in ('NUMBER','INTEGER','FLOAT','DOUBLE','DECIMAL','NUMERIC','BIGINT','INT','REAL') %}
                        min({{ col.name }}) as {{ col.name }}_min,
                        max({{ col.name }}) as {{ col.name }}_max,
                        avg({{ col.name }}) as {{ col.name }}_avg,
                        {% if include_advanced_stats %}
                            stddev({{ col.name }}) as {{ col.name }}_stddev,
                            variance({{ col.name }}) as {{ col.name }}_variance,
                            -- Percentiles (adapter-specific)
                            {% if target.type == 'snowflake' %}
                                percentile_cont(0.25) within group (order by {{ col.name }}) as {{ col.name }}_p25,
                                percentile_cont(0.5) within group (order by {{ col.name }}) as {{ col.name }}_median,
                                percentile_cont(0.75) within group (order by {{ col.name }}) as {{ col.name }}_p75,
                            {% endif %}
                            -- Outlier detection (values beyond 3 standard deviations)
                            sum(case when abs({{ col.name }} - avg({{ col.name }}) over()) > 3 * stddev({{ col.name }}) over() then 1 else 0 end) as {{ col.name }}_outlier_count,
                        {% endif %}
                    {% endif %}

                    -- Enhanced data quality metrics for all columns
                    {% if include_advanced_stats %}
                        -- Completeness rate
                        round((count({{ col.name }}) * 100.0) / count(*), 2) as {{ col.name }}_completeness_rate,
                        -- Uniqueness rate
                        round((count(distinct {{ col.name }}) * 100.0) / count(*), 2) as {{ col.name }}_uniqueness_rate,
                        -- Cardinality category
                        case 
                            when count(distinct {{ col.name }}) = count(*) then 'UNIQUE'
                            when count(distinct {{ col.name }}) = 1 then 'CONSTANT'
                            when count(distinct {{ col.name }}) <= 10 then 'LOW_CARDINALITY'
                            when count(distinct {{ col.name }}) <= 100 then 'MEDIUM_CARDINALITY'
                            else 'HIGH_CARDINALITY'
                        end as {{ col.name }}_cardinality_category,
                    {% endif %}

                    -- Universal metrics for all columns
                    sum(case when {{ col.name }} is null then 1 else 0 end) as {{ col.name }}_null_count,
                    count(distinct {{ col.name }}) as {{ col.name }}_distinct_count,

                {% endfor %}

                -- Data freshness monitoring
                {% if date_column %}
                    datediff('hour', max({{ date_column }}), current_timestamp()) as hours_since_update,
                    case 
                        when datediff('hour', max({{ date_column }}), current_timestamp()) > {{ data_freshness_threshold_hours }} 
                        then 'STALE'
                        else 'FRESH'
                    end as freshness_status,
                    max({{ date_column }}) as latest_record_date
                {% else %}
                    null as hours_since_update,
                    null as freshness_status,
                    null as latest_record_date
                {% endif %}
            from base_data
        )
        -- Final result with incremental tracking fields
        select
            {{ profile_date_expr }} as profile_date,
            '{{ model_name }}' as model_name,
            '{{ target.name }}' as target_environment,
            '{{ target.type }}' as database_type,
            current_timestamp() as profiled_at,
            '{{ dbt_version }}' as dbt_version,
            '{{ profile_frequency }}' as profile_frequency,
            -- Generate unique profile run ID
            concat(
                '{{ target.name }}',
                '_',
                '{{ model_name }}',
                '_',
                to_char({{ profile_date_expr }}, 'YYYYMMDDHH24')
            ) as profile_run_id,
            -- Row-level metrics
            rm.total_rows,
            rm.distinct_rows,
            round((rm.total_rows - rm.distinct_rows) * 100.0 / nullif(rm.total_rows, 0), 2) as duplicate_rate,
            -- Sample information
            {% if sample_size %}
                {{ sample_size }} as sample_percentage,
                'sampled' as data_type,
            {% else %}
                null as sample_percentage,
                'full' as data_type,
            {% endif %}
            -- Column count
            {{ cols | length }} as total_columns,
            -- Calculate profile hash for change detection
            hash(
                rm.total_rows,
                rm.distinct_rows,
                {% for col in cols %}
                    cm.{{ col.name }}_null_count,
                    cm.{{ col.name }}_distinct_count
                    {%- if not loop.last -%},{%- endif -%}
                {% endfor %}
            ) as profile_hash,
            -- Column-level metrics
            cm.*
        from row_metrics rm
        cross join column_metrics cm
    {% endif %}

{% endmacro %}