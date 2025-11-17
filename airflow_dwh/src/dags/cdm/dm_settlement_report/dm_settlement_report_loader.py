from pydantic import BaseModel
from datetime import datetime, timezone, date
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from lib.dict_util import json2str
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository


class ReportObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class ReportSrcRepository:
    def __init__(self, pg: PgConnect):
        self._db = pg

    def list_reports(self, limit: int) -> List[ReportObj]:
        with self._db.client().cursor(row_factory=class_row(ReportObj)) as cur:
            cur.execute(
                """
                SELECT
                    r.id AS restaurant_id,
                    r.restaurant_name,
                    t.ts::date AS settlement_date,
                    COUNT(DISTINCT o.id) AS orders_count,
                    SUM(f.total_sum)::numeric(14,2) AS orders_total_sum,
                    SUM(f.bonus_payment)::numeric(14,2) AS orders_bonus_payment_sum,
                    SUM(f.bonus_grant)::numeric(14,2) AS orders_bonus_granted_sum,
                    (SUM(f.total_sum) * 0.25)::numeric(14,2) AS order_processing_fee,
                    (SUM(f.total_sum) - SUM(f.bonus_payment) - (SUM(f.total_sum) * 0.25))::numeric(14,2)
                        AS restaurant_reward_sum
                FROM dds.dm_orders o
                JOIN dds.fct_product_sales f
                  ON f.order_id = o.id
                JOIN dds.dm_restaurants r
                  ON r.id = o.restaurant_id
                JOIN dds.dm_timestamps t
                  ON t.id = o.timestamp_id
                WHERE o.order_status = 'CLOSED'
                GROUP BY r.id, r.restaurant_name, t.ts::date
                ORDER BY settlement_date DESC, r.restaurant_name
                LIMIT %(limit)s;
                """,
                {"limit": limit}
            )
            return cur.fetchall()


class ReportDestRepository:
    def insert_report(self, conn: Connection, report: ReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report(
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                VALUES (
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(settlement_date)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(orders_bonus_payment_sum)s,
                    %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s,
                    %(restaurant_reward_sum)s
                )
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": report.restaurant_id,
                    "restaurant_name": report.restaurant_name,
                    "settlement_date": report.settlement_date,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "orders_bonus_payment_sum": report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": report.orders_bonus_granted_sum,
                    "order_processing_fee": report.order_processing_fee,
                    "restaurant_reward_sum": report.restaurant_reward_sum,
                },
            )


class ReportLoader:
    WF_KEY = "dm_settlement_report_workflow"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.src = ReportSrcRepository(pg_dwh)
        self.dest = ReportDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="cdm")
        self.log = log

    def load_reports(self):
        with self.pg_dwh.connection() as conn:
            load_queue = self.src.list_reports(self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for report in load_queue:
                self.dest.insert_report(conn, report)

            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings={"last_loaded": datetime.now(timezone.utc).isoformat()},
            )
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last loaded at {wf_setting.workflow_settings['last_loaded']}")
