{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}
WITH src_review AS (
    SELECT * FROM {{ ref('src_review') }}
)
SELECT * FROM src_review
WHERE review_text IS NOT NULL
{% if is_incremental() %}
AND review_date > (SELECT MAX(review_date) FROM {{this}})
{%endif%}
