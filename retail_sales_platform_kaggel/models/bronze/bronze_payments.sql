{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
) }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('staging', 'payments') }}

{% if is_incremental() %}
    WHERE order_id NOT IN (
        SELECT DISTINCT order_id FROM {{ this }}
    )
{% endif %}