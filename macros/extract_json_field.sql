
{%- macro extract_json_field(field_name, json_keys) -%}
    {{ return(adapter.dispatch('extract_json_field')(field_name, json_keys)) }}
{%- endmacro -%}

{%- macro default__extract_json_field(field_name, json_keys) -%}
    {#- https://docs.aws.amazon.com/redshift/latest/dg/JSON_EXTRACT_PATH_TEXT.html -#}
    {#- https://www.sqliz.com/postgresql-ref/json_extract_path_text/ -#}
    {#- Need to separate out the field numbers -#}
    {%- set field_with_numbers = [] -%}
    {%- for field in json_keys -%}
        {%- if '[' in field -%}
            {#- Get the field name -#}
            {%- set field_name = field.split('[')[0] -%}
            {%- do field_with_numbers.append(field_name) -%}
            {#- Get the field numbers -#}
            {%- for field_number in field.split('[')[1:] -%}
                {%- do field_with_numbers.append(field_number.replace(']', '')) -%}
            {%- endfor -%}
        {%- else -%}
            {%- do field_with_numbers.append(field) -%}
        {%- endif -%}
    {%- endfor -%}
    {#- Create the array of fields with the join in them -#}
    {%- set fields = field_with_numbers | join("', '") -%}
    {#- Create the extract path -#}
    json_extract_path_text({{ field_name }}::json, '{{ fields }}')
{%- endmacro -%}

{%- macro snowflake__extract_json_field(field_name, json_keys) -%}
    {#- https://docs.snowflake.com/en/sql-reference/functions/json_extract_path_text -#}

    {#- Create the array of fields with the join in them -#}
    {%- set fields = json_keys | join(".") -%}
    {#- Create the extract path -#}
    json_extract_path_text({{ field_name }}, '{{ fields }}')
{%- endmacro -%}

{%- macro bigquery__extract_json_field(field_name, json_keys) -%}
    {#- https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_query -#}

    {#- Create the array of fields with the join in them -#}
    {%- set fields = json_keys | join(".") -%}
    {#- Create the extract path -#}
    json_query({{ field_name }}, '$.{{ fields }}')
{%- endmacro -%}
