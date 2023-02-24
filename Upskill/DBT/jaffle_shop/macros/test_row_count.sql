{% test row_count(model, column_name, above) %}
    select row_count
    from (select count({{ column_name }}) as row_count from {{ model }}) A
    where row_count <= {{ above }}
{% endtest %}