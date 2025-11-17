from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect, MongoConnect
from lib.dict_util import json2str
from settings_repository import EtlSetting, EtlSettingsRepository


class OrderObj(BaseModel):
    id: str
    update_ts: datetime
    object_value: Dict[str, Any]


class OrdersOriginRepository:
    def __init__(self, mc: MongoConnect) -> None:
        self._dbs = mc.client()

    def list_orders(self, threshold: datetime, limit: int) -> List[OrderObj]:
        filter = {"update_ts": {"$gt": threshold}}
        sort = [("update_ts", 1)]
        docs = list(self._dbs.get_collection("orders").find(filter=filter, sort=sort, limit=limit))
        return [
            OrderObj(
                id=str(d["_id"]),
                update_ts=d["update_ts"],
                object_value=d,
            )
            for d in docs
        ]


class OrdersDestRepository:
    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        str_val = json2str(order.object_value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": order.id, "val": str_val, "update_ts": order.update_ts},
            )


class OrderLoader:
    WF_KEY = "ordersystem_orders_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000
    LOG_THRESHOLD = 100

    def __init__(self, origin: OrdersOriginRepository, pg_dest: PgConnect, dest: OrdersDestRepository, log: Logger) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_orders(self) -> int:
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

            load_queue = self.origin.list_orders(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            for i, order in enumerate(load_queue, start=1):
                self.dest.insert_order(conn, order)
                if i % self.LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} orders...")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(o.update_ts for o in load_queue).isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
            return len(load_queue)
