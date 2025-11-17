CREATE TABLE IF NOT EXISTS dds.dm_users (
    id SERIAL CONSTRAINT dm_users_pk PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    user_name VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL,
    CONSTRAINT dm_users_user_id_uq UNIQUE (user_id)
);
