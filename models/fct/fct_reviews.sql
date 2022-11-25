--models/fct/fct_reviews.sql
-- materializing as table
{{
  config(
    materialized = 'table'
    )
}}
WITH src_reviews AS (
 SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null
