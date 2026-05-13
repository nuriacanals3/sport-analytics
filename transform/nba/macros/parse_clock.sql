{% macro parse_clock(clock_col, period_duration_seconds=720) %}
    case
        when {{ clock_col }} is null then null
        else (
            {{ period_duration_seconds }}
            - (
                cast(regexp_extract({{ clock_col }}, 'PT(\d+)M', 1) as integer) * 60
                + cast(regexp_extract({{ clock_col }}, 'M(\d+(?:\.\d+)?)S', 1) as float)
            )
        )
    end
{% endmacro %}
