CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id SERIAL CONSTRAINT ordersystem_users_pk PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id)
);
