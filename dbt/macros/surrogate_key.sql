{# Local surrogate-key macro so we don't depend on the dbt_utils package.
   Mirrors dbt_utils.generate_surrogate_key: md5 of the fields, nulls coalesced. #}
{% macro make_sk(fields) %}
    md5(
        {%- for f in fields %}
        coalesce(cast({{ f }} as varchar), '_null_')
        {%- if not loop.last %} || '-' || {% endif %}
        {%- endfor %}
    )
{% endmacro %}
