{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
) }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,

    -- Was this paid in installments?
    CASE
        WHEN payment_installments > 1 THEN TRUE
        ELSE FALSE
    END AS is_installment,

    -- Categorize payment type
    CASE
        WHEN payment_type = 'credit_card' THEN 'Credit Card'
        WHEN payment_type = 'boleto' THEN 'Boleto'
        WHEN payment_type = 'voucher' THEN 'Voucher'
        WHEN payment_type = 'debit_card' THEN 'Debit Card'
        ELSE 'Other'
    END AS payment_type_clean

FROM {{ ref('bronze_payments') }}

{% if is_incremental() %}
    WHERE order_id NOT IN (
        SELECT DISTINCT order_id FROM {{ this }}
    )
{% endif %}