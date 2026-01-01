{% macro generate_schema_name(custom_schema_name, node) %}
  {# 
    Use exactly the schema defined in +schema.
    If not defined, fallback to the profile schema. 
  #}
  {{ custom_schema_name if custom_schema_name else node.schema }}
{% endmacro %}
