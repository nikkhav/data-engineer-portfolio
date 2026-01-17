import uuid
from datetime import datetime
from logging import Logger
from typing import List

from lib.pg.pg_connect import PgConnect
from repos.settings_repository import EtlSettingsRepository, EtlSetting
from repos.errors_repository import ErrorsRepository

from .origin_repo import DealsOriginRepository
from .dest_repo import DealsDestRepository
from .deal_obj import DealObj


class DealsExtractor:
    WF_KEY = "deals_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(
        self,
        origin: DealsOriginRepository,
        pg: PgConnect,
        dest: DealsDestRepository,
        errors: ErrorsRepository,
        log: Logger,
        batch_limit: int = 500,
    ):
        self.origin = origin
        self.pg = pg
        self.dest = dest
        self.errors = errors
        self.log = log
        self.batch_limit = batch_limit
        self.settings_repo = EtlSettingsRepository(schema="stg")

    def extract_deals(self, target_date: datetime) -> int:
        """
        Main ETL extraction method.
        1. loads workflow settings
        2. calls origin_repo (with retry)
        3. batch inserts using COPY
        4. updates workflow settings
        5. logs errors to stg.load_errors
        """

        batch_id = uuid.uuid4()

        with self.pg.connection() as conn:
            wf_setting = self.settings_repo.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: target_date.isoformat()},
                )

            # 1. EXTRACT
            try:
                deals: List[DealObj] = self.origin.list_deals(
                    target_date=target_date,
                    ingestion_id=batch_id,
                )
            except Exception as e:
                self.errors.insert_error(
                    conn=conn,
                    workflow_key=self.WF_KEY,
                    record={"target_date": target_date.isoformat()},
                    error_message=str(e),
                    error_type="extract_error",
                    ingestion_id=batch_id,
                    stacktrace=None,
                )
                raise

            if not deals:
                self.log.info(f"No deals found for {target_date.date()}")
                return 0

            # 2. INSERT (batch)
            try:
                self.dest.insert_batch(conn, deals)
            except Exception as e:
                for d in deals:
                    self.errors.insert_error(
                        conn=conn,
                        workflow_key=self.WF_KEY,
                        record={"transaction_number": d.transaction_number},
                        error_message=str(e),
                        error_type="load_error",
                        ingestion_id=batch_id,
                        stacktrace=None,
                    )
                raise

            # 3. Update workflow settings
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = target_date.isoformat()
            self.settings_repo.save_setting(conn, self.WF_KEY, wf_setting.workflow_settings)

            self.log.info(
                f"Loaded {len(deals)} deals for {target_date.date()}, batch_id={batch_id}"
            )

            return len(deals)