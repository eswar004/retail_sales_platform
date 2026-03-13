{{ config(
    materialized='table',
    schema='bronze'
) }}

SELECT
    PRODUCT_CATEGORY_NAME,
    PRODUCT_CATEGORY_NAME_ENGLISH
FROM {{ source('staging', 'product_category_translation') }}