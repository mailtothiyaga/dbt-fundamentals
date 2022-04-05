{% macro flatten_json(model_name, json_column, startswith_filter) %}

{% set json_column_query %}
select distinct
    regexp_replace(f.path, '\\\\[[0-9]+\\\\]', '[]') as full_path,
    REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')  as column_name--, 
    --typeof(f.value) as column_type

from {{ model_name }},

        lateral flatten({{ json_column }}, recursive=>true) f
where typeof(f.value) <> 'OBJECT' and startswith(full_path, '{{ startswith_filter }}')
order by 1
{% endset %}


{% set results = run_query(json_column_query) %}

--{{ log("results: " ~ results, True) }}

{% if execute %}
{% set results_list = results.rows.values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

--{{ log("results_list: " ~ results_list, True) }}

select 

{% for full_path, column_name in results_list %}
{{ json_column }}:{{ full_path }}::varchar as {{ column_name }}
{% if not loop.last %},{% endif %}
{% endfor %}

from {{ model_name }}

{% endmacro %}