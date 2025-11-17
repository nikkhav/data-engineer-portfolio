CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL NOT NULL,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INT NOT NULL,
    settlement_month INT NOT NULL,
    orders_count INT NOT NULL DEFAULT 0,
    orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    rate_avg NUMERIC(2,1) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- Первичный ключ
    CONSTRAINT pk_dm_courier_ledger PRIMARY KEY (id),

    -- Уникальность по курьеру + периоду
    CONSTRAINT uq_dm_courier_ledger_period UNIQUE (courier_id, settlement_year, settlement_month),

    -- Проверки на корректность года/месяца
    CONSTRAINT chk_dm_courier_ledger_year CHECK (settlement_year >= 2022 AND settlement_year < 2500),
    CONSTRAINT chk_dm_courier_ledger_month CHECK (settlement_month BETWEEN 1 AND 12),

    -- Проверки на положительные значения
    CONSTRAINT chk_dm_courier_ledger_orders_count CHECK (orders_count >= 0),
    CONSTRAINT chk_dm_courier_ledger_orders_total_sum CHECK (orders_total_sum >= 0),
    CONSTRAINT chk_dm_courier_ledger_order_processing_fee CHECK (order_processing_fee >= 0),
    CONSTRAINT chk_dm_courier_ledger_courier_order_sum CHECK (courier_order_sum >= 0),
    CONSTRAINT chk_dm_courier_ledger_courier_tips_sum CHECK (courier_tips_sum >= 0),
    CONSTRAINT chk_dm_courier_ledger_courier_reward_sum CHECK (courier_reward_sum >= 0),

    -- Проверка на рейтинг (0..5)
    CONSTRAINT chk_dm_courier_ledger_rate CHECK (rate_avg >= 0 AND rate_avg <= 5)
);
