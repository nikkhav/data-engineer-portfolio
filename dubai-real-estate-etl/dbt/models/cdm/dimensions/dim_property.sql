{{ config(materialized='table') }}

WITH properties AS (
    SELECT
        p.property_hk,
        p.project,
        p.area,
        p.property_type,
        p.property_subtype,
        p.usage_type,
        p.rooms,
        p.parking,
        p.freehold,
        p.offplan,
        p.area_sqm,
        p.load_ts
    FROM {{ ref('sat_property_details') }} p
)

SELECT *
FROM properties
ORDER BY property_hk