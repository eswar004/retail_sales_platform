{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

SELECT
    review_id,
    order_id,
    review_score,

    -- Sentiment bucket based on score
    {{ sentiment('review_score') }} AS sentiment,

    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,

    -- How many hours did it take to answer the review?
    DATEDIFF(
        'hour',
        review_creation_date,
        review_answer_timestamp
    ) AS review_response_time_hours

FROM {{ ref('bronze_order_reviews') }}

{% if is_incremental() %}
    WHERE review_creation_date > (
        SELECT COALESCE(MAX(review_creation_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}