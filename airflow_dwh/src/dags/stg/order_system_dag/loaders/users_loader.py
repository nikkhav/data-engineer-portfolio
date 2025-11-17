from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect, MongoConnect
from lib.dict_util import json2str
from settings_repository import EtlSetting, EtlSettingsRepository


class UserObj(BaseModel):
    id: str
    update_ts: datetime
    object_value: Dict[str, Any]


class UsersOriginRepository:
    def __init__(self, mc: MongoConnect) -> None:
        self._dbs = mc.client()

    def list_users(self, threshold: datetime, limit: int) -> List[UserObj]:
        filter = {"update_ts": {"$gt": threshold}}
        sort = [("update_ts", 1)]
        docs = list(self._dbs.get_collection("users").find(filter=filter, sort=sort, limit=limit))
        return [
            UserObj(
                id=str(d["_id"]),
                update_ts=d["update_ts"],
                object_value=d,
            )
            for d in docs
        ]


class UsersDestRepository:
    def insert_user(self, conn: Connection, user: UserObj) -> None:
        str_val = json2str(user.object_value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": user.id, "val": str_val, "update_ts": user.update_ts},
            )


class UserLoader:
    WF_KEY = "ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000
    LOG_THRESHOLD = 100

    def __init__(self, origin: UsersOriginRepository, pg_dest: PgConnect, dest: UsersDestRepository, log: Logger) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_users(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()},
                )

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            self.log.info(f"Starting from last checkpoint: {last_loaded_ts}")

            load_queue = self.origin.list_users(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            for i, user in enumerate(load_queue, start=1):
                self.dest.insert_user(conn, user)
                if i % self.LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} users...")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(u.update_ts for u in load_queue).isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
            return len(load_queue)
