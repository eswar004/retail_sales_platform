{{ config(
    materialized='incremental',
    unique_key='seller_id'
) }}

SELECT
    seller_id,
    seller_zip_code_prefix,

    -- Clean city and state
    UPPER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state

FROM {{ ref('bronze_sellers') }}

{% if is_incremental() %}
    WHERE seller_id NOT IN (
        SELECT seller_id FROM {{ this }}
    )
{% endif %}