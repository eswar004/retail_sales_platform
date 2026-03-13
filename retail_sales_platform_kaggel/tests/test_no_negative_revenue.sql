-- Test fails if any order has negative revenue
SELECT
    order_id,
    total_revenue
FROM {{ ref('fact_orders') }}
WHERE total_revenue < 0
