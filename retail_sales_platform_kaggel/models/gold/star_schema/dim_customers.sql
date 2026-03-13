{# {{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('obt_orders') }}
WHERE customer_unique_id IS NOT NULL #}
{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    customer_unique_id,
    customer_city,
    customer_state
FROM (
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp DESC
        ) AS rn
    FROM {{ ref('obt_orders') }}
    WHERE customer_unique_id IS NOT NULL
)
WHERE rn = 1