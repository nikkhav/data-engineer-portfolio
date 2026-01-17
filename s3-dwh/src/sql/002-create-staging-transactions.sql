CREATE TABLE IF NOT EXISTS VT25112277C9AC__STAGING.transactions (
    operation_id VARCHAR,
    account_number_from INT,
    account_number_to INT,
    currency_code SMALLINT,
    country VARCHAR,
    status VARCHAR,
    transaction_type VARCHAR,
    amount INT,
    transaction_dt TIMESTAMP
)

ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id) ALL NODES;