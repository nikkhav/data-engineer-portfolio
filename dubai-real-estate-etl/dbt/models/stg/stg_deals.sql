{{ config(materialized='view') }}

select
    transaction_number,
    payload,
    load_source,
    ingestion_id,
    load_ts
from stg.raw_deals