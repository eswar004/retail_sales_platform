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
    freight_value,

    -- Total value of this line item including freight
    ROUND(price + freight_value, 2) AS total_with_freight,

    -- Flag high value items above R$500
    {{ is_high_value('price', 500) }} AS is_high_value_item,

    -- Freight as percentage of item price
    CASE
        WHEN price > 0
        THEN ROUND((freight_value / price) * 100, 2)
        ELSE 0
    END AS freight_pct_of_price

FROM {{ ref('bronze_order_items') }}

{% if is_incremental() %}
    WHERE shipping_limit_date > (
        SELECT COALESCE(MAX(shipping_limit_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}