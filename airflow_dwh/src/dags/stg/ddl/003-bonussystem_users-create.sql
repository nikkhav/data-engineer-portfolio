CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id INTEGER CONSTRAINT bonussystem_users_pk PRIMARY KEY,
	order_user_id TEXT NOT NULL
);
