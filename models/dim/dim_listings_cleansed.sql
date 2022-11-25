--models/dim/dim_listings_cleansed.sql
WITH src_listings AS (
    SELECT * FROM {{ ref('src_listings') }}
)
SELECT 
  listing_id,
  listing_name,
  room_type,
  --replace all minimum night with 0 as 1
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  --convert to number and remove the $ symbol
  REPLACE(price_str,'$') :: NUMBER(10,2) AS price,
  created_at,
  updated_at
FROM
  src_listings
