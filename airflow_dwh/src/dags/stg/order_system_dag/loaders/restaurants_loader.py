from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect, MongoConnect
from lib.dict_util import json2str
from settings_repository import EtlSetting, EtlSettingsRepository


class RestaurantObj(BaseModel):
    id: str
    update_ts: datetime
    object_value: Dict[str, Any]


class RestaurantsOriginRepository:
    def __init__(self, mc: MongoConnect) -> None:
        self._dbs = mc.client()

    def list_restaurants(self, threshold: datetime, limit: int) -> List[RestaurantObj]:
        filter = {"update_ts": {"$gt": threshold}}
        sort = [("update_ts", 1)]
        docs = list(self._dbs.get_collection("restaurants").find(filter=filter, sort=sort, limit=limit))
        return [
            RestaurantObj(
                id=str(d["_id"]),
                update_ts=d["update_ts"],
                object_value=d,
            )
            for d in docs
        ]


class RestaurantsDestRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        str_val = json2str(restaurant.object_value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": restaurant.id, "val": str_val, "update_ts": restaurant.update_ts},
            )


class RestaurantLoader:
    WF_KEY = "ordersystem_restaurants_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000
    LOG_THRESHOLD = 100

    def __init__(
        self,
        origin: RestaurantsOriginRepository,
        pg_dest: PgConnect,
        dest: RestaurantsDestRepository,
        log: Logger,
    ) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_restaurants(self) -> int:
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

            load_queue = self.origin.list_restaurants(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            for i, restaurant in enumerate(load_queue, start=1):
                self.dest.insert_restaurant(conn, restaurant)
                if i % self.LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} restaurants...")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(r.update_ts for r in load_queue).isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
            return len(load_queue)
