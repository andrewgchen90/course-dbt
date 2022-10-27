{%- macro event_type_counts(table_name, column_name, column_count) -%}

    {% set event_types = dbt_utils.get_column_values(
        table = ref( table_name ),
        column = column_name
    ) %}

    {%- for event_type in event_types -%}
        count(case when {{ column_name }} = '{{ event_type }}' then {{ column_count }} else null end) as {{event_type}}_count
        {%- if not loop.last -%},
        {%- endif -%}
    {%- endfor -%}

{%- endmacro -%}