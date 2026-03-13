{{ config(
    materialized='incremental',
    unique_key='seller_id'
) }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('staging', 'sellers') }}

{% if is_incremental() %}
    WHERE seller_id NOT IN (
        SELECT seller_id FROM {{ this }}
    )
{% endif %}