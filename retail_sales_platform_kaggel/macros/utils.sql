{% macro is_late(estimated_col, actual_col) %}
    CASE
        WHEN {{ actual_col }} > {{ estimated_col }} THEN TRUE
        ELSE FALSE
    END
{% endmacro %}

{% macro sentiment(score_col) %}
    CASE
        WHEN {{ score_col }} >= 4 THEN 'positive'
        WHEN {{ score_col }} = 3 THEN 'neutral'
        ELSE 'negative'
    END
{% endmacro %}

{% macro is_high_value(amount_col, threshold) %}
    CASE
        WHEN {{ amount_col }} > {{ threshold }} THEN TRUE
        ELSE FALSE
    END
{% endmacro %}