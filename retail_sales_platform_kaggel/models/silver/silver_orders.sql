{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    customer_id,
    order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    -- How many days did delivery actually take?
    DATEDIFF(
        'day',
        order_purchase_timestamp,
        order_delivered_customer_date
    ) AS delivery_time_days,

    -- Was the order delivered after the estimated date?
    {{ is_late(
        'order_estimated_delivery_date',
        'order_delivered_customer_date'
    ) }} AS is_late,

    -- How many hours did approval take?
    DATEDIFF(
        'hour',
        order_purchase_timestamp,
        order_approved_at
    ) AS approval_time_hours

FROM {{ ref('bronze_orders') }}

{% if is_incremental() %}
    WHERE order_purchase_timestamp > (
        SELECT COALESCE(MAX(order_purchase_timestamp), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}