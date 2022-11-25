--models/dim/dim_hosts_cleansed.sql
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts') }}
)
SELECT
    host_id,
    --replace null with Anonymous for host_name column
    NVL(
        host_name,
        'Anonymous' 
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
