{% macro increment_select_access_count(table_name) %}
{% set counter_key = "select_access_counts_" ~ table_name %}
{% set current_count = var(counter_key, default=0) %}
{% set updated_count = current_count + 1 %}
{% set query = "create temp table if not exists temp_select_access_counts (table_name text, count integer); insert into temp_select_access_counts (table_name, count) values ('" ~ table_name ~ "', " ~ updated_count ~ ") on conflict (table_name) do update set count = excluded.count;" %}
{% do run_query(query) %}
{% endmacro %}

{% macro read_select_access_count(table_name) %}
{% set counter_key = "select_access_counts_" ~ table_name %}
{% set result = run_query("select coalesce((select count from temp_select_access_counts where table_name = '" ~ table_name ~ "'), 0);") %}
{{ result[0]['coalesce'] }}
{% endmacro %}