{% call statement('tables_for_union', fetch_result=True) %}
    SELECT table_name FROM information_schema.tables WHERE table_schema='rrdata'
{% endcall %}

{% set tables = load_result('tables_for_union')['data'] %}

with source_data as (
    {% for table in tables %}
        SELECT 
            to_json(rrdata.trucktrips.*) as jsonData,
            CURRENT_TIMESTAMP as last_updated,
            0 as processed
        FROM {{ ref(table[0]) }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * 
from source_data