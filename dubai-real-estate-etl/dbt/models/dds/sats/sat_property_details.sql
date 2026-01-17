{{ config(
    materialized='incremental',
    unique_key='property_hk'
) }}

WITH src AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} as property_hk,

        payload ->> 'PROJECT_EN'        AS project,
        payload ->> 'AREA_EN'           AS area,
        payload ->> 'PROP_TYPE_EN'      AS property_type,
        payload ->> 'PROP_SB_TYPE_EN'   AS property_subtype,
        payload ->> 'USAGE_EN'          AS usage_type,
        payload ->> 'ROOMS_EN'          AS rooms,
        payload ->> 'PARKING'           AS parking,
        payload ->> 'IS_FREE_HOLD_EN'   AS freehold,
        payload ->> 'IS_OFFPLAN_EN'     AS offplan,

        (payload ->> 'ACTUAL_AREA')::float AS area_sqm,

        payload ->> 'MASTER_PROJECT_EN'    AS master_project,
        payload ->> 'NEAREST_MALL_EN'      AS nearest_mall,
        payload ->> 'NEAREST_METRO_EN'     AS nearest_metro,
        payload ->> 'NEAREST_LANDMARK_EN'  AS nearest_landmark,

        load_ts,

        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'USAGE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'PARKING'",
            "payload ->> 'IS_FREE_HOLD_EN'",
            "payload ->> 'IS_OFFPLAN_EN'",
            "payload ->> 'ACTUAL_AREA'",
            "payload ->> 'MASTER_PROJECT_EN'",
            "payload ->> 'NEAREST_MALL_EN'",
            "payload ->> 'NEAREST_METRO_EN'",
            "payload ->> 'NEAREST_LANDMARK_EN'"
        ]) }} as hashdiff

    FROM {{ ref('stg_deals') }}
)

SELECT * FROM src