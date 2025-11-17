from pydantic import BaseModel
from datetime import datetime
from lib import PgConnect
from typing import List, Optional
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class ProductObjDds(BaseModel):
    id: Optional[int] = None
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class ProductsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, threshold: int, limit: int) -> List[ProductObjDds]:
        with self._db.client().cursor(row_factory=class_row(ProductObjDds)) as cur:
            cur.execute(
                """
                    SELECT
                        r.id,
                        (menu_item->>'_id')::text AS product_id,
                        (menu_item->>'name')::text AS product_name,
                        (menu_item->'price')::numeric(14,2) AS product_price,
                        r.update_ts AS active_from,
                        '2099-12-31 00:00:00'::timestamp AS active_to,
                        dr.id AS restaurant_id
                    FROM stg.ordersystem_restaurants r
                    CROSS JOIN LATERAL jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
                    JOIN dds.dm_restaurants dr
                      ON dr.restaurant_id = (r.object_value::json->>'_id')
                     AND dr.active_to = '2099-12-31 00:00:00'
                    WHERE r.id > %(threshold)s
                    ORDER BY r.id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class ProductDestRepository:
    def insert_product(self, conn: Connection, product: ProductObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(
                        product_id, product_name, product_price, restaurant_id, active_from, active_to
                    )
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        restaurant_id = EXCLUDED.restaurant_id,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "restaurant_id": product.restaurant_id,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )


class ProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = ProductsStgRepository(pg_dwh)
        self.dest = ProductDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_products(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for product in load_queue:
                self.dest.insert_product(conn, product)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
