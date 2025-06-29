{% macro learn_variables() %}

    {# Jinja variable #}
    {% set project_name = "dbtlearn" %}
    {{ log("This project is " ~ project_name, info=True) }}

    {# DBT variable#}
    {{ log("And was created by " ~ var("user_name"), info=True) }}

{% endmacro %}