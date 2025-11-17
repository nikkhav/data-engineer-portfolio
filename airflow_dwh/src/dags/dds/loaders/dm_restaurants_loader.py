from pydantic import BaseModel
from datetime import datetime
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository
from lib.dict_util import json2str


class RestaurantObjDds(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class RestaurantsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, threshold: int, limit: int) -> List[RestaurantObjDds]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObjDds)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        (object_value::jsonb ->> '_id')::text    AS restaurant_id,
                        (object_value::jsonb ->> 'name')::text   AS restaurant_name,
                        update_ts                                AS active_from,
                        '2099-12-31 00:00:00'::timestamp         AS active_to
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit}
            )
            return cur.fetchall()


class RestaurantDestRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(
                        restaurant_id, restaurant_name, active_from, active_to
                    )
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to,
                },
            )


class RestaurantLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = None

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = RestaurantsStgRepository(pg_dwh)
        self.dest = RestaurantDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="dds")
        self.log = log

    def load_restaurants(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restaurant in load_queue:
                self.dest.insert_restaurant(conn, restaurant)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(obj.id for obj in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
