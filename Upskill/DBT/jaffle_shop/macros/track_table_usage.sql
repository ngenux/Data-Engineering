{% macro track_table_usage(table_name) %}
    {% set current_timestamp = current_timestamp() %}
    {% set usage_query %}
        SELECT
            '{{table_name}}' AS table_name,
            '{{current_timestamp}}' AS timestamp,
            COUNT(*) AS row_count
        FROM {{table_name}}
    {% endset %}
    {{ execute(usage_query) }}
{% endmacro %}