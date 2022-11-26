--models/fct/fct_reviews.sql
{{
 config(
 materialized = 'incremental',
 on_schema_change='fail'
 )
}}
WITH src_reviews AS (
 SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

-----------------------------
--DBT RUN the model
--------------------------------
--add the below code in fct_reviews.sql at the end

{% if is_incremental() %}
 AND review_date > (select max(review_date) from {{ this }})
{% endif %}

------------------------------------
--Add the row from snowflake worksheet
--------------------------------------
--INSERT INTO "AIRBNB"."RAW"."RAW_REVIEWS" VALUES (3176, CURRENT_TIMESTAMP(), 'Zoltan', 'excellent stay!', 'positive');
-----------------------------------
--DBT RUN the model again
--SELECT * FROM "AIRBNB"."DEV"."RAW_REVIEWS" WHERE LISTING_ID = 3176;

-- you would see the newly added row in the dev schema without table is getting dropped