{{ config(materialized='view') }}

SELECT
    pr.area,
    pr.property_type,
    ROUND(AVG(f.price_per_sqm_aed)::numeric, 2) AS avg_ppsm,
    COUNT(*) AS deals
FROM {{ ref('fact_deals') }} f
JOIN {{ ref('dim_property') }} pr ON f.property_hk = pr.property_hk
GROUP BY 1,2
ORDER BY avg_ppsm DESC