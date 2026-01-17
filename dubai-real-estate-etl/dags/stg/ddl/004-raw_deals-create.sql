CREATE TABLE IF NOT EXISTS stg.raw_deals (
    id SERIAL PRIMARY KEY,
    transaction_number VARCHAR NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    load_ts TIMESTAMP NOT NULL DEFAULT NOW(),
    load_source VARCHAR NOT NULL,
    ingestion_id UUID NOT NULL
);
