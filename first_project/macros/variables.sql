{% macro learn_variables() -%}
    {% set my_variable = "Hello, " ~ var("user_name") ~ "!" %}
    {{ log("The value of my_variable is: " ~ var("user_name"), info=True) }}
{%- endmacro %}

