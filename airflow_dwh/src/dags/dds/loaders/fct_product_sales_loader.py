from pydantic import BaseModel
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class ProductSalesObjDds(BaseModel):
    stg_id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float
    courier_id: int


class ProductSalesStgRepository:
    def __init__(self, pg):
        self._db = pg

    def list_product_sales(self, threshold: int, limit: int) -> List[ProductSalesObjDds]:
        with self._db.client().cursor(row_factory=class_row(ProductSalesObjDds)) as cur:
            cur.execute(
                """
                SELECT
                    o.id AS stg_id,
                    p.id AS product_id,
                    o_dds.id AS order_id,
                    (oi.value ->> 'quantity')::int AS count,
                    (oi.value ->> 'price')::numeric(14,2) AS price,
                    ((oi.value ->> 'price')::numeric(14,2) * (oi.value ->> 'quantity')::int) AS total_sum,
                    (bp.value ->> 'bonus_payment')::numeric(14,2) AS bonus_payment,
                    (bp.value ->> 'bonus_grant')::numeric(14,2) AS bonus_grant,
                    c.id AS courier_id
                FROM stg.ordersystem_orders o
                CROSS JOIN LATERAL jsonb_array_elements(o.object_value::jsonb -> 'order_items') AS oi(value)
                JOIN dds.dm_orders o_dds
                  ON o_dds.order_id = (o.object_value::jsonb ->> '_id')::text
                JOIN dds.dm_products p
                  ON p.product_id = (oi.value ->> 'id')
                JOIN stg.bonussystem_events be
                  ON be.event_type = 'bonus_transaction'
                  AND (be.event_value::jsonb ->> 'order_id') = (o.object_value::jsonb ->> '_id')
                JOIN stg.deliverysystem_deliveries d ON (o.object_value::jsonb ->> '_id')::text = (d.object_value::jsonb ->> 'order_id')
                JOIN dds.dm_couriers c ON c.courier_id = (d.object_value::jsonb ->> 'courier_id')::text
                CROSS JOIN LATERAL jsonb_array_elements(be.event_value::jsonb -> 'product_payments') AS bp(value)
                WHERE (bp.value ->> 'product_id') = (oi.value ->> 'id')
                  AND o.id > %(threshold)s
                ORDER BY o.id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class ProductSalesDestRepository:
    def insert_order(self, conn: Connection, product_sales: ProductSalesObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(
                        product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant, courier_id
                    )
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s, %(courier_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant,
                        courier_id = EXCLUDED.courier_id;
                """,
                {
                    "product_id": product_sales.product_id,
                    "order_id": product_sales.order_id,
                    "count": product_sales.count,
                    "price": product_sales.price,
                    "total_sum": product_sales.total_sum,
                    "bonus_payment": product_sales.bonus_payment,
                    "bonus_grant": product_sales.bonus_grant,
                    "courier_id": product_sales.courier_id,
                },
            )


class ProductSalesLoader:
    WF_KEY = "product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = ProductSalesStgRepository(pg_dwh)
        self.dest = ProductSalesDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_orders(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} product sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                self.dest.insert_order(conn, order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.stg_id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
