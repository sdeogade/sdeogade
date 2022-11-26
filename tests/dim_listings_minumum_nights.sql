--tests/dim_listings_minumum_nights.sql

--In Singular test, if the query returns any rows, then test will get failed.
SELECT
 *
FROM {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10