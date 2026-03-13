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
    order_estimated_delivery_date
FROM {{ source('staging', 'orders') }}

{% if is_incremental() %}
    WHERE order_purchase_timestamp > (
        SELECT COALESCE(MAX(order_purchase_timestamp), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}