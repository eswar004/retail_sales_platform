{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    payment_type_clean,
    is_installment
FROM {{ ref('silver_payments') }}
WHERE payment_type_clean IS NOT NULL