CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id SERIAL CONSTRAINT ordersystem_restaurants_pk PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id)
);
