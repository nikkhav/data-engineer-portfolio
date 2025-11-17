CREATE TABLE IF NOT EXISTS dds.dm_products (
    id SERIAL CONSTRAINT dm_products_pk PRIMARY KEY,
    product_id VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT dm_products_price_check CHECK (product_price >= 0),
    restaurant_id INT NOT NULL,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL,
    CONSTRAINT dm_products_restaurant_id_fkey
    FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_products_product_id_uq UNIQUE (product_id)
);
