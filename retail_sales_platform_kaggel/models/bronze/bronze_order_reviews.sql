{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM {{ source('staging', 'order_reviews') }}

{% if is_incremental() %}
    WHERE review_creation_date > (
        SELECT COALESCE(MAX(review_creation_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}
