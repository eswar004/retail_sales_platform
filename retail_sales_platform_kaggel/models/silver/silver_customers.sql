{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,

    -- Clean city and state to uppercase
    UPPER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state

FROM {{ ref('bronze_customers') }}

{% if is_incremental() %}
    WHERE customer_id NOT IN (
        SELECT customer_id FROM {{ this }}
    )
{% endif %}