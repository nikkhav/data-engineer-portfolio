{{ config(materialized='table') }}

WITH deals AS (
    SELECT
        t.transaction_hk,
        tp.property_hk,

        t.transaction_value_aed,
        p.area_sqm,
        t.transaction_value_aed / NULLIF(p.area_sqm, 0) AS price_per_sqm_aed,

        t.transaction_date,
        t.procedure_type,

        to_char(t.transaction_date, 'YYYYMMDD')::int AS date_key
    FROM {{ ref('sat_transaction_details') }} t
    JOIN {{ ref('link_transaction_property') }} tp
      ON t.transaction_hk = tp.transaction_hk
    JOIN {{ ref('sat_property_details') }} p
      ON tp.property_hk = p.property_hk
)

SELECT *
FROM deals