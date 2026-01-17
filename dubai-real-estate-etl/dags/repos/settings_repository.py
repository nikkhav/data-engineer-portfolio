from typing import Dict, Optional
from psycopg2.extensions import connection as Connection
from psycopg2.extras import DictCursor
from pydantic import BaseModel
import json


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:
    def __init__(self, schema: str) -> None:
        allowed_schemas = {"stg", "dds", "cdm"}
        if schema not in allowed_schemas:
            raise ValueError(f"Invalid schema: {schema}")
        self.schema = schema

    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                f"""
                    SELECT id, workflow_key, workflow_settings
                    FROM {self.schema}.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            row = cur.fetchone()
            if not row:
                return None
            return EtlSetting(**row)

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: Dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {self.schema}.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings
                    ;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": json.dumps(workflow_settings),
                },
            )