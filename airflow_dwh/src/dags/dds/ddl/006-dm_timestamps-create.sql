CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id SERIAL CONSTRAINT dm_timestamps_pk PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    year SMALLINT NOT NULL CONSTRAINT dm_timestamps_year_check CHECK (year >= 2022 AND year < 2500),
    month SMALLINT NOT NULL CONSTRAINT dm_timestamps_month_check CHECK (month >= 1 AND month <= 12),
    day SMALLINT NOT NULL CONSTRAINT dm_timestamps_day_check CHECK (day >= 1 AND day <= 31),
    time TIME NOT NULL,
    date DATE NOT NULL
);
