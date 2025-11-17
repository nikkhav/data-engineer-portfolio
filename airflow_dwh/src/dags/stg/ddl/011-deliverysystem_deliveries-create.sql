CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id SERIAL CONSTRAINT deliverysystem_deliveries_pk PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE (object_id)
);
