{{ config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id']
) }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM {{ source('staging', 'order_items') }}

{% if is_incremental() %}
    WHERE shipping_limit_date > (
        SELECT COALESCE(MAX(shipping_limit_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}