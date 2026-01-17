CREATE TABLE IF NOT EXISTS VT25112277C9AC__STAGING.currencies (
    currency_code SMALLINT,
    currency_code_with SMALLINT,
    currency_with_div NUMERIC(4,2),
    date_update DATE
)

ORDER BY date_update
SEGMENTED BY HASH(currency_code, currency_code_with) ALL NODES;