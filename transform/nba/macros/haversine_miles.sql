{# Great-circle distance in miles between two lat/lon points (haversine formula).
   Standard approximation, not a real flight route -- declared in the travel-logistics plan.
   Nulls propagate naturally: if any input is null, the result is null. #}
{% macro haversine_miles(lat1, lon1, lat2, lon2) %}
    (
        2 * 3958.8 * asin(
            sqrt(
                pow(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
                + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
                * pow(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
            )
        )
    )
{% endmacro %}
