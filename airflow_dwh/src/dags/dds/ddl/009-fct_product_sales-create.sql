CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id SERIAL CONSTRAINT fct_product_sales_pk PRIMARY KEY,
    product_id INT NOT NULL,
    order_id INT NOT NULL,
    count INT DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
    price NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
    total_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
    bonus_payment NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
    bonus_grant NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
    courier_id INT NOT NULL,
    CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products(id),
    CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_product_sales_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);
