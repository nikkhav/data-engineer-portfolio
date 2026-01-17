import logging
import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from lib.vertica.vertica_connect import VerticaConnect
from lib.pg.pg_connect import PgConnect
from lib.settings_repository import WfSettingsRepository
from stg.s3_origin_repository import S3OriginRepository
from stg.currencies_loader import CurrenciesLoader
from stg.transactions_loader import TransactionsLoader

log = logging.getLogger(__name__)

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=True,
    tags=['stg', 'source', 's3'],
    is_paused_upon_creation=False
)
def staging_ingestion_dag():

    @task(task_id="currencies_ingestion")
    def currencies_ingestion_task(logical_date=None):
        log.info("Start currencies ingestion")

        s3_client = boto3.client(
            "s3",
            endpoint_url=Variable.get("S3_ENDPOINT"),
            aws_access_key_id=Variable.get("S3_KEY_ID"),
            aws_secret_access_key=Variable.get("S3_SECRET")
        )

        bucket = Variable.get("S3_BUCKET")

        s3_repo = S3OriginRepository(s3_client, bucket, log)
        wf_repo = WfSettingsRepository("stg")
        vertica = VerticaConnect("VERTICA_DWH_CONNECTION")
        pg = PgConnect("PG_DWH_CONNECTION")

        loader = CurrenciesLoader(
            s3_repo=s3_repo,
            vertica_conn=vertica,
            pg_conn=pg,
            wf_repo=wf_repo,
            logger=log
        )

        rows = loader.load()
        return f"Loaded currencies: {rows}"

    @task(task_id="transactions_ingestion")
    def transactions_ingestion_task(logical_date=None):
        log.info("Start transactions ingestion")

        s3_client = boto3.client(
            "s3",
            endpoint_url=Variable.get("S3_ENDPOINT"),
            aws_access_key_id=Variable.get("S3_KEY_ID"),
            aws_secret_access_key=Variable.get("S3_SECRET")
        )

        bucket = Variable.get("S3_BUCKET")

        s3_repo = S3OriginRepository(s3_client, bucket, log)
        wf_repo = WfSettingsRepository("stg")
        vertica = VerticaConnect("VERTICA_DWH_CONNECTION")
        pg = PgConnect("PG_DWH_CONNECTION")

        loader = TransactionsLoader(
            s3_repo=s3_repo,
            vertica_conn=vertica,
            pg_conn=pg,
            wf_repo=wf_repo,
            logger=log
        )

        rows = loader.load()
        return f"Loaded transactions: {rows}"

    currencies = currencies_ingestion_task()
    transactions = transactions_ingestion_task()

    currencies >> transactions


staging_ingestion_dag = staging_ingestion_dag()
