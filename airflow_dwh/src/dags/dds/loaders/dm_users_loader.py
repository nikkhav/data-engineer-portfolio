from pydantic import BaseModel
from lib import PgConnect
from datetime import datetime
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class UserObjDds(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UsersStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, threshold: int, limit: int):
        with self._db.client().cursor(row_factory=class_row(UserObjDds)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        (object_value::jsonb ->> '_id')::text       AS user_id,
                        (object_value::jsonb ->> 'name')::text      AS user_name,
                        (object_value::jsonb ->> 'login')::text     AS user_login
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class UserDestRepository:
    def insert_user(self, conn: Connection, user: UserObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login,
                },
            )


class UserLoader:
    WF_KEY = "users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = UsersStgRepository(pg_dwh)
        self.dest = UserDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_users(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1},
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dest.insert_user(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
