{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    -- Keys
    order_id,
    customer_id,
    customer_unique_id,

    -- Order timestamps
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    -- Order metrics
    order_status,
    delivery_time_days,
    approval_time_hours,
    is_late,

    -- Item metrics
    {# total_items,
    total_revenue,
    total_freight,
    total_order_value,
    has_high_value_item, #}
    COALESCE(total_items, 0)        AS total_items,
    COALESCE(total_revenue, 0)      AS total_revenue,
    COALESCE(total_freight, 0)      AS total_freight,
    COALESCE(total_order_value, 0)  AS total_order_value,
    COALESCE(has_high_value_item, 0) AS has_high_value_item,

    -- Payment metrics
    payment_type_clean,
    is_installment,
    total_payment_value,
    payment_installments,

    -- Review metrics
    review_score,
    sentiment,
    review_response_time_hours

FROM {{ ref('obt_orders') }}