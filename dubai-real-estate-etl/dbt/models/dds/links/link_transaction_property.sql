{{ config(
    materialized='incremental',
    unique_key='link_hk'
) }}

WITH src AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['transaction_number']) }} AS transaction_hk,

        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} AS property_hk,

        {{ dbt_utils.generate_surrogate_key([
            "transaction_number",
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} AS link_hk,

        load_ts AS load_dts
    FROM {{ ref('stg_deals') }}
)

SELECT DISTINCT
    link_hk,
    transaction_hk,
    property_hk,
    load_dts
FROM src