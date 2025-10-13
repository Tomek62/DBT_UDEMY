{% macro learn_logging() -%}
    {{ log("This is a log message from the learn_logging macro.", info=True) }}
{%- endmacro %}