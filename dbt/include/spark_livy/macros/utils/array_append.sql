{% macro spark_livy__array_append(array, new_element) -%}
    {% if env_var('DBT_SPARK_VERSION') == "2" %}
        {{ exceptions.raise_compiler_error("Array functions need Spark 3") }}
    {% else %}
        {{ array_concat(array, array_construct([new_element])) }}
    {% endif %}
{%- endmacro %}
