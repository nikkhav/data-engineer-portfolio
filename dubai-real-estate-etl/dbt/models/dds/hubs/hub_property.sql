{{ config(materialized='incremental', unique_key='property_hk') }}

WITH src AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} AS property_hk,
        load_ts AS load_dts
    FROM {{ ref('stg_deals') }}
)

SELECT * FROM src