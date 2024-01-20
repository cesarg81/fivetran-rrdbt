{% call statement('tables_for_union', fetch_result=True) %}
    SELECT table_name FROM information_schema.tables WHERE table_schema='rrdata' and table_name != 'combined_data'
{% endcall %}

{% set tables = load_result('tables_for_union')['data'] %}

{{ config(materialized='incremental', dist='id') }}

with source_data as (
    {% for table in tables %}
        SELECT 
            '{{ table[0] }}' as origin_table,
            to_json({{ source('postgres', table[0]) }}.*) as jsonData,
            CURRENT_TIMESTAMP as last_updated,
            0 as processed
        FROM {{ source('postgres', table[0]) }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * 
from source_data