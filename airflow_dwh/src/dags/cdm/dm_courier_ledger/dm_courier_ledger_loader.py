from pydantic import BaseModel
from datetime import datetime, timezone, date
from lib import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from lib.dict_util import json2str
from logging import Logger
from settings_repository import EtlSetting, EtlSettingsRepository


class LedgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float



class LedgerSrcRepository:
    def __init__(self, pg: PgConnect):
        self._db = pg

    def list_ledgers(self, limit: int) -> List[LedgerObj]:
        with self._db.client().cursor(row_factory=class_row(LedgerObj)) as cur:
            cur.execute(
                """
                WITH courier_stats AS (SELECT c.id                                       AS courier_id,
                                              c.courier_name,
                                              EXTRACT(YEAR FROM t.ts)                    AS settlement_year,
                                              EXTRACT(MONTH FROM t.ts)                   AS settlement_month,
                                              COUNT(DISTINCT d.id)                       AS orders_count,
                                              SUM(ps.total_sum)::numeric(14, 2)          AS orders_total_sum,
                                              AVG(d.rate)::numeric(3, 2)                 AS rate_avg,
                                              (SUM(ps.total_sum) * 0.25)::numeric(14, 2) AS order_processing_fee,
                                              SUM(d.tip_amount)::numeric(14, 2)          AS courier_tips_sum
                                       FROM dds.fct_deliveries d
                                                JOIN dds.dm_couriers c ON c.id = d.courier_id
                                                JOIN dds.dm_orders o ON o.id = d.order_id
                                                JOIN dds.fct_product_sales ps ON ps.order_id = o.id
                                                JOIN dds.dm_timestamps t ON t.id = o.timestamp_id
                                       WHERE o.order_status = 'CLOSED'
                                       GROUP BY c.id, c.courier_name, settlement_year, settlement_month)
                SELECT cs.*,
                       -- courier_order_sum
                       GREATEST(
                               CASE
                                   WHEN cs.rate_avg < 4.0 THEN cs.orders_total_sum * 0.05
                                   WHEN cs.rate_avg < 4.5 THEN cs.orders_total_sum * 0.07
                                   WHEN cs.rate_avg < 4.9 THEN cs.orders_total_sum * 0.08
                                   ELSE cs.orders_total_sum * 0.10
                                   END,
                               CASE
                                   WHEN cs.rate_avg < 4.0 THEN 100
                                   WHEN cs.rate_avg < 4.5 THEN 150
                                   WHEN cs.rate_avg < 4.9 THEN 175
                                   ELSE 200
                                   END
                       )     AS courier_order_sum,
                       -- courier_reward_sum
                       (
                           GREATEST(
                                   CASE
                                       WHEN cs.rate_avg < 4.0 THEN cs.orders_total_sum * 0.05
                                       WHEN cs.rate_avg < 4.5 THEN cs.orders_total_sum * 0.07
                                       WHEN cs.rate_avg < 4.9 THEN cs.orders_total_sum * 0.08
                                       ELSE cs.orders_total_sum * 0.10
                                       END,
                                   CASE
                                       WHEN cs.rate_avg < 4.0 THEN 100
                                       WHEN cs.rate_avg < 4.5 THEN 150
                                       WHEN cs.rate_avg < 4.9 THEN 175
                                       ELSE 200
                                       END
                           ) + cs.courier_tips_sum * 0.95
                           ) AS courier_reward_sum
                FROM courier_stats cs
                ORDER BY cs.settlement_year DESC, cs.settlement_month DESC, cs.courier_name
                LIMIT %(limit)s;
                """,
                {"limit": limit}
            )
            return cur.fetchall()


class LedgerDestRepository:
    def insert_ledger(self, conn: Connection, report: LedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger(courier_id,
                                                  courier_name,
                                                  settlement_year,
                                                  settlement_month,
                                                  orders_count,
                                                  orders_total_sum,
                                                  rate_avg,
                                                  order_processing_fee,
                                                  courier_order_sum,
                                                  courier_tips_sum,
                                                  courier_reward_sum)
                VALUES (%(courier_id)s,
                        %(courier_name)s,
                        %(settlement_year)s,
                        %(settlement_month)s,
                        %(orders_count)s,
                        %(orders_total_sum)s,
                        %(rate_avg)s,
                        %(order_processing_fee)s,
                        %(courier_order_sum)s,
                        %(courier_tips_sum)s,
                        %(courier_reward_sum)s)
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET courier_name         = EXCLUDED.courier_name,
                        orders_count         = EXCLUDED.orders_count,
                        orders_total_sum     = EXCLUDED.orders_total_sum,
                        rate_avg             = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum    = EXCLUDED.courier_order_sum,
                        courier_tips_sum     = EXCLUDED.courier_tips_sum,
                        courier_reward_sum   = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": report.courier_id,
                    "courier_name": report.courier_name,
                    "settlement_year": report.settlement_year,
                    "settlement_month": report.settlement_month,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "rate_avg": report.rate_avg,
                    "order_processing_fee": report.order_processing_fee,
                    "courier_order_sum": report.courier_order_sum,
                    "courier_tips_sum": report.courier_tips_sum,
                    "courier_reward_sum": report.courier_reward_sum
                }
            )


class LedgerLoader:
    WF_KEY = "dm_courier_ledger_workflow"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.src = LedgerSrcRepository(pg_dwh)
        self.dest = LedgerDestRepository()
        self.settings_repository = EtlSettingsRepository(schema="cdm")
        self.log = log

    def load_ledgers(self):
        with self.pg_dwh.connection() as conn:
            load_queue = self.src.list_ledgers(self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for ledger in load_queue:
                self.dest.insert_ledger(conn, ledger)

            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings={"last_loaded": datetime.now(timezone.utc).isoformat()},
            )
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished. Last loaded at {wf_setting.workflow_settings['last_loaded']}")
