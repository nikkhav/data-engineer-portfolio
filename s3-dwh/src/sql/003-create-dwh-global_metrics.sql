CREATE TABLE IF NOT EXISTS VT25112277C9AC__DWH.global_metrics (
    date_update DATE,
    currency_from SMALLINT,
    amount_total NUMERIC(20,4),
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC(10,4),
    cnt_accounts_make_transactions INT
)

ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;