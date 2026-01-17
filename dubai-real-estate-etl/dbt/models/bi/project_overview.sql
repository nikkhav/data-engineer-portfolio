{{ config(materialized='view') }}

SELECT
    p.project_name,
    p.area_name,
    COUNT(*) AS deals_count,
    ROUND(AVG(f.price_per_sqm_aed)::numeric, 2) AS avg_ppsm,
    ROUND(MIN(f.price_per_sqm_aed)::numeric, 2) AS min_ppsm,
    ROUND(MAX(f.price_per_sqm_aed)::numeric, 2) AS max_ppsm,
    ROUND(AVG(f.area_sqm)::numeric, 2) AS avg_area,
    SUM(CASE WHEN pr.offplan = 'Off-Plan' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS offplan_share
FROM {{ ref('fact_deals') }} f
JOIN {{ ref('dim_property') }} pr ON f.property_hk = pr.property_hk
JOIN {{ ref('dim_project') }} p ON pr.project = p.project_name
GROUP BY 1,2
ORDER BY avg_ppsm DESC