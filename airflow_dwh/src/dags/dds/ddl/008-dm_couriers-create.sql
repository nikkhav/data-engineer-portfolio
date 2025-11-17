CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL CONSTRAINT dm_couriers_pk PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    CONSTRAINT dm_couriers_courier_id_uq UNIQUE (courier_id)
);
