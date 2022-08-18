{% macro safe_divide(num, denom) %}
    case when {{ denom }} = 0 then 0 else {{ num }} / {{ denom }} end
{% endmacro %}