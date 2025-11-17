CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id SERIAL CONSTRAINT dm_orders_pk PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    order_status VARCHAR NOT NULL,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    CONSTRAINT dm_orders_order_id_uq UNIQUE (order_id),
    CONSTRAINT dm_orders_user_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
    CONSTRAINT dm_orders_restaurant_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);
