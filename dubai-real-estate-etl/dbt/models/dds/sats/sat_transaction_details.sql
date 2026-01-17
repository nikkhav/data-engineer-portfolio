{{ config(materialized='incremental', unique_key='transaction_hk') }}

WITH src AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['transaction_number']) }} AS transaction_hk,

        (payload ->> 'INSTANCE_DATE')::timestamp AS transaction_date,
        payload ->> 'PROCEDURE_EN' AS procedure_type,
        (payload ->> 'TRANS_VALUE')::float AS transaction_value_aed,

        load_ts,

        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'INSTANCE_DATE'",
            "payload ->> 'PROCEDURE_EN'",
            "payload ->> 'TRANS_VALUE'"
        ]) }} AS hashdiff
    FROM {{ ref('stg_deals') }}
)

SELECT * FROM src