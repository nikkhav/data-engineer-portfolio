from pydantic import BaseModel
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class OrderObjDds(BaseModel):
    stg_id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_id: str
    order_status: str


class OrdersStgRepository:
    def __init__(self, pg):
        self._db = pg

    def list_orders(self, threshold: int, limit: int) -> List[OrderObjDds]:
        with self._db.client().cursor(row_factory=class_row(OrderObjDds)) as cur:
            cur.execute(
                """
                SELECT 
                    o.id AS stg_id,
                    u.id AS user_id,
                    r.id AS restaurant_id,
                    t.id AS timestamp_id,
                    (o.object_value::jsonb ->> '_id')::text AS order_id,
                    (o.object_value::jsonb ->> 'final_status')::text AS order_status
                FROM stg.ordersystem_orders o
                JOIN dds.dm_users u 
                  ON u.user_id = (o.object_value::jsonb -> 'user' ->> 'id')::text
                JOIN dds.dm_restaurants r
                    ON r.restaurant_id = (o.object_value::jsonb -> 'restaurant' ->> 'id')::text
                JOIN dds.dm_timestamps t
                    ON t.ts = (o.object_value::jsonb ->> 'date')::timestamp
                WHERE o.id > %(threshold)s
                ORDER BY o.id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class OrderDestRepository:
    def insert_order(self, conn: Connection, order: OrderObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_id, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_id)s, %(order_status)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_id = EXCLUDED.order_id,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "order_id": order.order_id,
                    "order_status": order.order_status
                },
            )


class OrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = OrdersStgRepository(pg_dwh)
        self.dest = OrderDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_orders(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                self.dest.insert_order(conn, order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.stg_id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
