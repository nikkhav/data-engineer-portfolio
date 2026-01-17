CREATE LOCAL TEMP TABLE tmp_src ON COMMIT PRESERVE ROWS AS
WITH tx_with_rates AS (
    SELECT
        t.transaction_dt::date AS date_update,
        t.currency_code AS currency_from,
        t.account_number_from,
        t.amount,
        c.currency_with_div,
        ROW_NUMBER() OVER (
            PARTITION BY t.operation_id, t.transaction_dt
            ORDER BY c.date_update DESC
        ) AS rn
    FROM VT25112277C9AC__STAGING.transactions t
    JOIN VT25112277C9AC__STAGING.currencies c
        ON t.currency_code = c.currency_code
        AND c.date_update <= t.transaction_dt::date
    WHERE t.transaction_dt::date = :target_date
        AND t.account_number_from > 0
        AND t.account_number_to > 0
),
daily_transactions AS (
    SELECT
        date_update,
        currency_from,
        account_number_from,
        amount,
        currency_with_div
    FROM tx_with_rates
    WHERE rn = 1
),
tx_per_account AS (
    SELECT
        account_number_from,
        date_update,
        currency_from,
        COUNT(*) AS cnt_transactions_per_account
    FROM daily_transactions
    GROUP BY account_number_from, date_update, currency_from
)
SELECT
    dt.date_update,
    dt.currency_from,
    SUM(dt.amount * dt.currency_with_div) AS amount_total,
    COUNT(*) AS cnt_transactions,
    AVG(ta.cnt_transactions_per_account) AS avg_transactions_per_account,
    COUNT(DISTINCT dt.account_number_from) AS cnt_accounts_make_transactions
FROM daily_transactions dt
JOIN tx_per_account ta
    ON dt.account_number_from = ta.account_number_from
    AND dt.date_update = ta.date_update
    AND dt.currency_from = ta.currency_from
GROUP BY dt.date_update, dt.currency_from
ORDER BY dt.date_update, dt.currency_from;

MERGE INTO VT25112277C9AC__DWH.global_metrics AS tgt
USING tmp_src AS src
ON tgt.date_update = src.date_update
   AND tgt.currency_from = src.currency_from
WHEN MATCHED THEN UPDATE SET
    amount_total = src.amount_total,
    cnt_transactions = src.cnt_transactions,
    avg_transactions_per_account = src.avg_transactions_per_account,
    cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN INSERT (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
) VALUES (
    src.date_update,
    src.currency_from,
    src.amount_total,
    src.cnt_transactions,
    src.avg_transactions_per_account,
    src.cnt_accounts_make_transactions
);