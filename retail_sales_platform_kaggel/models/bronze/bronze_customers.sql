{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('staging', 'customers') }}

{% if is_incremental() %}
    WHERE customer_id NOT IN (
        SELECT customer_id FROM {{ this }}
    )
{% endif %}