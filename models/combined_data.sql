{% call statement('tables_for_union', fetch_result=True) %}
    SELECT table_name FROM information_schema.tables WHERE table_schema='rrdata' and table_name != 'combined_data'
{% endcall %}

{% set tables = load_result('tables_for_union')['data'] %}

{{
    config(
        materialized='incremental',
        unique_key='row_hash'
    )
}}

with source_data as (
    {% for table in tables %}
        SELECT 
            '{{ table[0] }}' as origin_table,
            to_json(tmp.*) as json_data,
            md5(CAST(to_json(tmp.*) AS TEXT)) AS row_hash,
            CURRENT_TIMESTAMP as last_updated,
            0 as processed
        FROM ( 
            SELECT 
                {{ dbt_utils.star(from=source('postgres', table[0]), except=["_fivetran_synced"]) }}
            FROM 
                {{ source('postgres', table[0]) }} 
            WHERE "_fivetran_synced" >= CURRENT_TIMESTAMP - INTERVAL '15 minutes' 
        ) tmp
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * 
from source_data