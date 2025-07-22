{% macro analyze_downstream_impact(exclude_patterns=['^profile.*', '.*lineage$']) %}
    {% if execute %}
        {% set lineage_data = [] %}
        
        -- First pass: Build a comprehensive map of all models and their direct downstream dependencies
        {% set downstream_map = {} %}
        {% set all_models = [] %}
        
        -- Collect all non-excluded models first
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' %}
                {% set model_name = node.name %}

                {% for pat in exclude_patterns %}
                    {% if modules.re.match(pat, model_name) %}
                        {% break %}
                    {% elif loop.last %}
                        {% set _ = all_models.append(model_name) %}
                        {% set _ = downstream_map.update({model_name: []}) %}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
        
        -- Second pass: Build the downstream relationships correctly
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' %}
                {% set model_name = node.name %}
                
                -- Only process non-excluded models
                {% if model_name in all_models %}
                    -- Check each dependency of the current model
                    {% for dep in node.depends_on.nodes %}
                        {% if dep.split('.')[0] == 'model' %}
                            {% set dep_model_name = dep.split('.')[2] %}
                            
                            -- If the dependency is also a non-excluded model,
                            -- then current model is downstream of that dependency
                            {% if dep_model_name in all_models %}
                                {% set _ = downstream_map[dep_model_name].append(model_name) %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% endfor %}
        
        -- Third pass: Calculate complete downstream impact for each model
        {% for model_name in all_models %}
            -- Get direct downstream models (immediate children)
            {% set direct_downstream = downstream_map[model_name] %}
            {% set direct_downstream_count = direct_downstream | length %}
            
            -- Calculate ALL downstream models recursively (complete subtree)
            {% set all_downstream = [] %}
            {% set visited = [] %}
            
            -- Use iterative approach with queue to avoid recursion limits
            {% set queue = direct_downstream.copy() %}
            
            -- Process all nodes in the downstream subtree
            {% for _ in range(100) %}  {# Safety limit for very deep DAGs #}
                {% if queue | length == 0 %}
                    {% break %}
                {% endif %}
                
                {% set current = queue.pop(0) %}
                
                -- Only process if not already visited
                {% if current not in visited %}
                    {% set _ = visited.append(current) %}
                    {% set _ = all_downstream.append(current) %}
                    
                    -- Add all downstream models of current model to queue
                    {% if current in downstream_map %}
                        {% for child in downstream_map[current] %}
                            {% if child not in visited and child not in queue %}
                                {% set _ = queue.append(child) %}
                            {% endif %}
                        {% endfor %}
                    {% endif %}
                {% endif %}
            {% endfor %}
            
            {% set total_downstream_count = all_downstream | length %}
            
            -- Determine model criticality based on total downstream count
            {% set model_criticality = 'leaf' %}
            
            {% if total_downstream_count == 0 %}
                {% set model_criticality = 'leaf' %}
            {% elif total_downstream_count == 1 %}
                {% set model_criticality = 'low_impact' %}
            {% elif total_downstream_count <= 3 %}
                {% set model_criticality = 'medium_impact' %}
            {% elif total_downstream_count <= 7 %}
                {% set model_criticality = 'high_impact' %}
            {% else %}
                {% set model_criticality = 'critical' %}
            {% endif %}
            
           -- Calculate depth levels for additional insights
            {% set max_depth = 0 %}
            
            {% if direct_downstream_count > 0 %}
                {% set depth_queue = [] %}
                {% set depth_tracker = [] %}  {# Track all depths encountered #}
                
                -- Initialize queue with direct children at depth 1
                {% for direct_child in direct_downstream %}
                    {% set _ = depth_queue.append({'node': direct_child, 'depth': 1}) %}
                    {% set _ = depth_tracker.append(1) %}
                {% endfor %}
                
                {% set depth_visited = [] %}
                
                -- Calculate depth
                {% for iteration in range(100) %}  {# Depth calculation limit #}
                    {% if depth_queue | length == 0 %}
                        {% break %}
                    {% endif %}
                    
                    {% set current_item = depth_queue.pop(0) %}
                    {% set current_node = current_item.node %}
                    {% set current_depth = current_item.depth %}
                    
                    {% if current_node not in depth_visited %}
                        {% set _ = depth_visited.append(current_node) %}
                        {% set _ = depth_tracker.append(current_depth) %}
                        
                        -- Add children with incremented depth
                        {% if current_node in downstream_map and downstream_map[current_node] | length > 0 %}
                            {% set next_depth = current_depth + 1 %}

                            {% for child in downstream_map[current_node] %}
                                {% if child not in depth_visited %}
                                    {% set _ = depth_queue.append({'node': child, 'depth': next_depth}) %}
                                {% endif %}
                            {% endfor %}
                        {% endif %}
                    {% endif %}
                {% endfor %}
                
                -- Calculate max_depth from all tracked depths
                {% if depth_tracker | length > 0 %}
                    {% set max_depth = depth_tracker | max %}
                {% endif %}
            {% endif %}
            
            {% set _ = lineage_data.append({
                'model_name': model_name,
                'direct_downstream_count': direct_downstream_count,
                'total_downstream_count': total_downstream_count,
                'direct_downstream_models': direct_downstream | join(', '),
                'all_downstream_models': all_downstream | join(', '),
                'model_criticality': model_criticality,
                'max_downstream_depth': max_depth
            }) %}
        {% endfor %}
        
        -- Sort lineage data by total downstream count descending (most critical first)
        {% set sorted_lineage = lineage_data | sort(attribute='total_downstream_count', reverse=true) %}
        
        -- Generate the final result set
        {% for item in sorted_lineage %}
            select 
                '{{ item.model_name }}' as model_name,
                {{ item.direct_downstream_count }} as direct_downstream_count,
                {{ item.total_downstream_count }} as total_downstream_count,
                '{{ item.direct_downstream_models }}' as direct_downstream_models,
                '{{ item.all_downstream_models }}' as all_downstream_models,
                '{{ item.model_criticality }}' as model_criticality,
                {{ item.max_downstream_depth }} as max_downstream_depth,
                case 
                    when {{ item.total_downstream_count }} > 10 then 'CRITICAL'
                    when {{ item.total_downstream_count }} > 7 then 'VERY_HIGH'
                    when {{ item.total_downstream_count }} > 3 then 'HIGH'
                    when {{ item.total_downstream_count }} > 1 then 'MEDIUM'
                    when {{ item.total_downstream_count }} > 0 then 'LOW'
                    else 'NONE'
                end as downstream_impact_level,
                case 
                    when {{ item.total_downstream_count }} > 10 then 'Critical Foundation - Changes will cascade through {{ item.total_downstream_count }} models across {{ item.max_downstream_depth }} levels, requires extreme care and comprehensive testing'
                    when {{ item.total_downstream_count }} > 7 then 'High Impact - Changes affect {{ item.total_downstream_count }} downstream models across {{ item.max_downstream_depth }} levels, extensive coordination needed'
                    when {{ item.total_downstream_count }} > 3 then 'Moderate Impact - {{ item.total_downstream_count }} models depend on this across {{ item.max_downstream_depth }} levels, plan changes carefully'
                    when {{ item.total_downstream_count }} > 1 then 'Low Impact - {{ item.total_downstream_count }} models depend on this across {{ item.max_downstream_depth }} levels, manageable change scope'
                    when {{ item.total_downstream_count }} > 0 then 'Minimal Impact - Only {{ item.total_downstream_count }} model depends on this'
                    else 'No Impact - Leaf node, changes only affect this model'
                end as impact_description,
                -- Calculate change risk score (higher downstream count + depth = higher risk)
                case 
                    when {{ item.total_downstream_count }} > 10 then 10
                    when {{ item.total_downstream_count }} > 7 then 8
                    when {{ item.total_downstream_count }} > 3 then 6
                    when {{ item.total_downstream_count }} > 1 then 4
                    when {{ item.total_downstream_count }} > 0 then 2
                    else 0
                end + least({{ item.max_downstream_depth }}, 3) as change_risk_score,
                -- Calculate business impact score
                case 
                    when {{ item.total_downstream_count }} > 7 then 'Business Critical'
                    when {{ item.total_downstream_count }} > 5 then 'High Business Value'
                    when {{ item.total_downstream_count }} > 3 then 'Moderate Business Value'
                    when {{ item.total_downstream_count }} > 1 then 'Supporting Role'
                    when {{ item.total_downstream_count }} > 0 then 'Limited Scope'
                    else 'End Consumer'
                end as business_impact_category,
                -- Additional insights
                case 
                    when {{ item.direct_downstream_count }} > 3 then 'High Fanout'
                    when {{ item.direct_downstream_count }} > 1 then 'Medium Fanout'
                    when {{ item.direct_downstream_count }} > 0 then 'Low Fanout'
                    else 'No Fanout'
                end as fanout_pattern,
                case 
                    when {{ item.max_downstream_depth }} > 2 then 'Deep Chain'
                    when {{ item.max_downstream_depth }} > 1 then 'Medium Chain'
                    when {{ item.max_downstream_depth }} > 0 then 'Shallow Chain'
                    else 'No Chain'
                end as chain_pattern,
                current_timestamp() as analyzed_at

            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}