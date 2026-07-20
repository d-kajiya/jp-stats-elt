{#
    Override dbt's default schema-naming behaviour.

    Default (postgres): final schema = <target.schema>_<custom_schema>,
    which yields ugly names like `staging_staging`, `marts_marts`.

    Here we use the custom schema (the model's +schema, e.g. `staging`)
    verbatim when one is set, and fall back to target.schema otherwise.
    Result: models land in `staging` / `intermediate` / `marts` directly.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}