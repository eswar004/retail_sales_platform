{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('silver_sellers') }}
WHERE seller_id IS NOT NULL