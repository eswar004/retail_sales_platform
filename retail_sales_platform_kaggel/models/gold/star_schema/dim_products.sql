{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    product_id,
    product_category_name,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3
FROM {{ ref('silver_products') }}
WHERE product_id IS NOT NULL