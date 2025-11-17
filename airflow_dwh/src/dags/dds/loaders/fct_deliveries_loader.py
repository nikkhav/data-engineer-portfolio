from pydantic import BaseModel
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str
from datetime import datetime


class DeliveriesObjDds(BaseModel):
    stg_id: int
    courier_id: int
    order_id: int
    delivery_ts: datetime
    rate: int
    tip_amount: float


class DeliveriesStgRepository:
    def __init__(self, pg):
        self._db = pg

    def list_deliveries(self, threshold: int, limit: int) -> List[DeliveriesObjDds]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesObjDds)) as cur:
            cur.execute(
                """
                SELECT
                    d.id AS stg_id,
                    c.id AS courier_id,
                    o.id AS order_id,
                    (d.object_value::jsonb ->> 'delivery_ts')::timestamp AS delivery_ts,
                    (d.object_value::jsonb ->> 'rate')::int AS rate,
                    (d.object_value::jsonb ->> 'tip_sum')::float AS tip_amount
                FROM stg.deliverysystem_deliveries d
                JOIN dds.dm_couriers c
                  ON (d.object_value::jsonb ->> 'courier_id')::text = c.courier_id
                JOIN dds.dm_orders o
                  ON (d.object_value::jsonb ->> 'order_id')::text = o.order_id
                WHERE d.id > %(threshold)s
                ORDER BY d.id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class DeliveriesDestRepository:
    def insert_delivery(self, conn: Connection, deliveries: DeliveriesObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(courier_id, order_id, delivery_ts, rate, tip_amount)
                    VALUES (%(courier_id)s, %(order_id)s, %(delivery_ts)s, %(rate)s, %(tip_amount)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        order_id = EXCLUDED.order_id,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        tip_amount = EXCLUDED.tip_amount;
                """,
                {
                    "courier_id": deliveries.courier_id,
                    "order_id": deliveries.order_id,
                    "delivery_ts": deliveries.delivery_ts,
                    "rate": deliveries.rate,
                    "tip_amount": deliveries.tip_amount,
                },
            )


class DeliveriesLoader:
    WF_KEY = "deliveries_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = DeliveriesStgRepository(pg_dwh)
        self.dest = DeliveriesDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_deliveries(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.dest.insert_delivery(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.stg_id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
