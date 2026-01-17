{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT
        t.transaction_date::date AS date,
        to_char(t.transaction_date, 'YYYYMMDD')::int AS date_key,
        EXTRACT(DAY FROM t.transaction_date) AS day,
        EXTRACT(MONTH FROM t.transaction_date) AS month,
        EXTRACT(YEAR FROM t.transaction_date) AS year,
        EXTRACT(QUARTER FROM t.transaction_date) AS quarter,
        TO_CHAR(t.transaction_date, 'Month') AS month_name,
        TO_CHAR(t.transaction_date, 'DY') AS day_name,
        CASE WHEN EXTRACT(DOW FROM t.transaction_date) IN (0, 6)
            THEN TRUE ELSE FALSE END AS is_weekend
    FROM {{ ref('sat_transaction_details') }} t
)

SELECT *
FROM dates
ORDER BY date