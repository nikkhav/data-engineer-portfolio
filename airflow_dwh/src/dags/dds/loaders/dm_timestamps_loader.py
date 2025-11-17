from pydantic import BaseModel
from datetime import datetime, date, time
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class TimestampObjDds(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, threshold: int, limit: int) -> List[TimestampObjDds]:
        with self._db.client().cursor(row_factory=class_row(TimestampObjDds)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        (object_value::json->>'date')::timestamp AS ts,
                        EXTRACT(YEAR  FROM (object_value::json->>'date')::timestamp) AS year,
                        EXTRACT(MONTH FROM (object_value::json->>'date')::timestamp) AS month,
                        EXTRACT(DAY   FROM (object_value::json->>'date')::timestamp) AS day,
                        (object_value::json->>'date')::date AS date,
                        (object_value::json->>'date')::timestamp::time AS time
                    FROM stg.ordersystem_orders
                    WHERE (object_value::json->>'final_status') IN ('CLOSED','CANCELLED')
                      AND id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class TimestampDestRepository:
    def insert_timestamp(self, conn: Connection, timestamp: TimestampObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )


class TimestampLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = TimestampsStgRepository(pg_dwh)
        self.dest = TimestampDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_timestamps(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for timestamp in load_queue:
                self.dest.insert_timestamp(conn, timestamp)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
