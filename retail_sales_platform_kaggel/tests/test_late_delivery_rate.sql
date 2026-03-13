{{ config(severity='warn') }}

-- Warn if late delivery rate exceeds 10%
WITH late_rate AS (
    SELECT
        ROUND(
            SUM(CASE WHEN is_late = TRUE THEN 1 ELSE 0 END) * 100.0
            / COUNT(*),
            2
        ) AS late_pct
    FROM {{ ref('fact_orders') }}
    WHERE order_status = 'delivered'
)
SELECT late_pct
FROM late_rate
WHERE late_pct > 10