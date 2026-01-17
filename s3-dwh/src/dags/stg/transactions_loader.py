import io

from stg.s3_origin_repository import S3OriginRepository
from lib.settings_repository import WfSettingsRepository

class TransactionsLoader:
    def __init__(
            self,
            s3_repo: S3OriginRepository,
            vertica_conn,
            pg_conn,
            wf_repo: WfSettingsRepository,
            logger
    ):
        self.s3_repo = s3_repo
        self.vertica_conn = vertica_conn
        self.pg_conn = pg_conn
        self.wf_repo = wf_repo
        self.log = logger

        self.workflow_key = "transactions_load"
        self.target_table = "VT25112277C9AC__STAGING.transactions"
        self.reject_table = "VT25112277C9AC__STAGING.transactions_rejects"

    def load(self):
        self.log.info("Loading transactions...")

        with self.pg_conn.connection() as pg:
            df, last_batch = self.s3_repo.list_transactions(
                pg_conn=pg,
                wf_repo=self.wf_repo,
                workflow_key=self.workflow_key
            )

        if df.empty:
            self.log.info("No new batches to load.")
            return 0

        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False, header=False)
        csv_data = csv_buf.getvalue()


        with self.vertica_conn.connection() as conn:
            cur = conn.cursor()
            cur.copy(
                f"""
                    COPY {self.target_table}
                    FROM STDIN
                    DELIMITER ','
                    REJECTED DATA AS TABLE {self.reject_table}
                """,
                csv_data
            )

        with self.pg_conn.connection() as pg:
            self.wf_repo.save_setting(
                pg,
                self.workflow_key,
                {"last_loaded_batch": last_batch}
            )
        self.log.info(f"Workflow state updated: last_loaded_batch = {last_batch}")

        self.log.info(f"Loaded {len(df)} records into {self.target_table}")
        return len(df)
