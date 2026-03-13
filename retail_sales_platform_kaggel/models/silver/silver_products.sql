{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

SELECT
    p.product_id,

    -- Use English category name if available, otherwise keep Portuguese
    COALESCE(
        UPPER(TRIM(t.PRODUCT_CATEGORY_NAME_ENGLISH)),
        UPPER(TRIM(p.product_category_name)),
        'UNKNOWN'
    ) AS product_category_name,

    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Product volume in cubic cm
    ROUND(
        p.product_length_cm * p.product_height_cm * p.product_width_cm,
        2
    ) AS product_volume_cm3

FROM {{ ref('bronze_products') }} p
LEFT JOIN {{ ref('bronze_product_category_translation') }} t
    ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.PRODUCT_CATEGORY_NAME))

{% if is_incremental() %}
    WHERE p.product_id NOT IN (
        SELECT product_id FROM {{ this }}
    )
{% endif %}