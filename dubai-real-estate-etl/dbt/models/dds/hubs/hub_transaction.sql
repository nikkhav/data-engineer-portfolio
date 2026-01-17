{{ config(materialized='incremental', unique_key='transaction_hk') }}

WITH src AS (
    SELECT
        transaction_number,
        ingestion_id,
        load_ts
    FROM {{ ref('stg_deals') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_number']) }} AS transaction_hk,
    transaction_number,
    ingestion_id,
    load_ts AS load_dts
FROM src