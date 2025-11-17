CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id SERIAL CONSTRAINT deliverysystem_couriers_pk PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE (object_id)
);
