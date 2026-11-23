{# Database ClickHouse = schema dbt. Pakai nama custom apa adanya (stg, gold), bukan gold_stg. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
