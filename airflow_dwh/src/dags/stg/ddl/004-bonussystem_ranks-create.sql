CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id INTEGER CONSTRAINT bonussystem_ranks_pk PRIMARY KEY,
	name VARCHAR NOT NULL,
	bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL
);
