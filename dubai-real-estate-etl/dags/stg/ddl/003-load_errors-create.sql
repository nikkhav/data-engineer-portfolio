CREATE TABLE IF NOT EXISTS stg.load_errors (
    id SERIAL PRIMARY KEY,
    workflow_key VARCHAR NOT NULL,
    source_record JSONB,
    error_message TEXT NOT NULL,
    error_type VARCHAR,
    error_ts TIMESTAMP NOT NULL DEFAULT NOW(),
    ingestion_id UUID,
    stacktrace TEXT
);