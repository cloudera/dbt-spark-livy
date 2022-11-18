{% macro spark_livy__array_construct(inputs, data_type) -%}
    {% if env_var('DBT_SPARK_VERSION') == "2" %}
        {{ exceptions.raise_compiler_error("Array functions need Spark 3") }}
    {% else %}
        array( {{ inputs|join(' , ') }} )
    {% endif %}
{%- endmacro %}
