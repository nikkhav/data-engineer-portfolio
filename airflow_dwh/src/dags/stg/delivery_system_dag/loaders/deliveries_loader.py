from datetime import datetime, timedelta
from logging import Logger
from typing import Any, Dict, List

import requests
from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str
from settings_repository import EtlSetting, EtlSettingsRepository


class DeliveryObj(BaseModel):
    id: str
    update_ts: datetime
    object_value: Dict[str, Any]


class DeliveriesOriginRepository:
    def __init__(self, api_credentials) -> None:
        self._api_credentials = api_credentials

    def list_deliveries(self, threshold: datetime, limit: int = 100) -> List[DeliveryObj]:
        # Загружаем данные за последние 7 дней
        from_ts = max(threshold, datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        to_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        params = {
            "from": from_ts,
            "to": to_ts,
            "sort_field": "date",
            "sort_direction": "asc",
            "limit": limit,
            "offset": 0
        }
        headers = {
            "X-Nickname": self._api_credentials.nickname,
            "X-Cohort": self._api_credentials.cohort,
            "X-API-KEY": self._api_credentials.api_key,
        }

        results: List[DeliveryObj] = []

        while True:
            resp = requests.get(f"{self._api_credentials.host}/deliveries", params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            batch = data if isinstance(data, list) else data.get("deliveries", [])
            if not batch:
                break

            for item in batch:
                order_ts = datetime.fromisoformat(item["order_ts"])
                results.append(
                    DeliveryObj(
                        id=item["delivery_id"],
                        update_ts=order_ts,
                        object_value=item
                    )
                )

            params["offset"] += limit

            if len(batch) < limit:
                break

        return results


class DeliveriesDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        str_val = json2str(delivery.object_value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": delivery.id, "val": str_val, "update_ts": delivery.update_ts},
            )


class DeliveryLoader:
    WF_KEY = "deliverysystem_deliveries_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000
    LOG_THRESHOLD = 100

    def __init__(self, origin: DeliveriesOriginRepository, pg_dest: PgConnect, dest: DeliveriesDestRepository, log: Logger) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_deliveries(self) -> int:
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

            load_queue = self.origin.list_deliveries(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            for delivery in load_queue:
                self.dest.insert_delivery(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(o.update_ts for o in load_queue).isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
            return len(load_queue)
