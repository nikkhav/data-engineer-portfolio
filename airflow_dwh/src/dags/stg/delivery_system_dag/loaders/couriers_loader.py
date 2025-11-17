from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

import requests
from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str
from settings_repository import EtlSettingsRepository


class CourierObj(BaseModel):
    id: str
    update_ts: datetime
    object_value: Dict[str, Any]


class CouriersOriginRepository:
    def __init__(self, api_credentials) -> None:
        self._api_credentials = api_credentials

    def list_couriers(self, limit: int = 100) -> List[CourierObj]:
        params = {
            "limit": limit,
            "offset": 0
        }
        headers = {
            "X-Nickname": self._api_credentials.nickname,
            "X-Cohort": self._api_credentials.cohort,
            "X-API-KEY": self._api_credentials.api_key,
        }

        results: List[CourierObj] = []
        now = datetime.now()

        while True:
            resp = requests.get(f"{self._api_credentials.host}/couriers", params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            batch = data if isinstance(data, list) else data.get("couriers", [])
            if not batch:
                break

            for item in batch:
                results.append(
                    CourierObj(
                        id=item["_id"],
                        update_ts=now,
                        object_value=item
                    )
                )

            params["offset"] += limit
            if len(batch) < limit:
                break

        return results


class CouriersDestRepository:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        str_val = json2str(courier.object_value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {"id": courier.id, "val": str_val, "update_ts": courier.update_ts},
            )


class CourierLoader:
    WF_KEY = "deliverysystem_couriers_origin_to_stg_workflow"
    BATCH_LIMIT = 1000

    def __init__(self, origin: CouriersOriginRepository, pg_dest: PgConnect, dest: CouriersDestRepository, log: Logger) -> None:
        self.origin = origin
        self.dest = dest
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema="stg")
        self.log = log

    def load_couriers(self) -> int:
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_couriers(self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            for courier in load_queue:
                self.dest.insert_courier(conn, courier)

            self.log.info("Load finished.")
            return len(load_queue)
