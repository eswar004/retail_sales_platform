{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    -- Order details
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.delivery_time_days,
    o.is_late,
    o.approval_time_hours,

    -- Customer details
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,

    -- Order items aggregated to order level
    oi.total_items,
    oi.total_revenue,
    oi.total_freight,
    oi.total_order_value,
    oi.has_high_value_item,

    -- Payment details
    p.payment_type_clean,
    p.is_installment,
    p.total_payment_value,
    p.payment_installments,

    -- Review details
    r.review_score,
    r.sentiment,
    r.review_response_time_hours

FROM {{ ref('silver_orders') }} o

LEFT JOIN {{ ref('silver_customers') }} c
    ON o.customer_id = c.customer_id

LEFT JOIN (
    SELECT
        order_id,
        COUNT(order_item_id)            AS total_items,
        ROUND(SUM(price), 2)            AS total_revenue,
        ROUND(SUM(freight_value), 2)    AS total_freight,
        ROUND(SUM(total_with_freight), 2) AS total_order_value,
        MAX(is_high_value_item::INT)    AS has_high_value_item
    FROM {{ ref('silver_order_items') }}
    GROUP BY order_id
) oi ON o.order_id = oi.order_id

LEFT JOIN (
    SELECT
        order_id,
        MAX(payment_type_clean)         AS payment_type_clean,
        MAX(is_installment::INT)        AS is_installment,
        SUM(payment_value)              AS total_payment_value,
        MAX(payment_installments)       AS payment_installments
    FROM {{ ref('silver_payments') }}
    GROUP BY order_id
) p ON o.order_id = p.order_id

LEFT JOIN (
    SELECT
        order_id,
        MAX(review_score)               AS review_score,
        MAX(sentiment)                  AS sentiment,
        AVG(review_response_time_hours) AS review_response_time_hours
    FROM {{ ref('silver_order_reviews') }}
    GROUP BY order_id
) r ON o.order_id = r.order_id