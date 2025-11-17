CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id INTEGER CONSTRAINT bonussystem_events_pk PRIMARY KEY,
	event_ts TIMESTAMP NOT NULL,
	event_type VARCHAR NOT NULL,
	event_value TEXT NOT NULL
);
