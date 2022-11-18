{% macro spark_livy___intersect() %}
    {% if env_var('DBT_SPARK_VERSION') == "2" %}
        {{ exceptions.raise_compiler_error("Array functions need Spark 3") }}
    {% else %}
        intersect
    {% endif %}
{% endmacro %}
