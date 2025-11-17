CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id SERIAL CONSTRAINT fct_deliveries_pk PRIMARY KEY,
    courier_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    rate INTEGER NOT NULL,
    tip_amount NUMERIC(14,2) NOT NULL,
    CONSTRAINT fct_deliveries_rate_check CHECK (rate >= 1 AND rate <= 5),
    CONSTRAINT fct_deliveries_courier_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
    CONSTRAINT fct_deliveries_order_fk FOREIGN KEY (order_id) REFERENCES dds.fct_product_sales(id)
);
