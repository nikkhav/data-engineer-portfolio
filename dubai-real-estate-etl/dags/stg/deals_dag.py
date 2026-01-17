import logging
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import dag, task

from lib.pg.pg_connect import PgConnect
from stg.deals.deals_extractor import DealsExtractor
from stg.deals.origin_repo import DealsOriginRepository
from stg.deals.dest_repo import DealsDestRepository
from repos.errors_repository import ErrorsRepository

log = logging.getLogger(__name__)

URL = "https://gateway.dubailand.gov.ae/open-data/transactions/export/csv"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['stg', 'source', 'extract'],
    is_paused_upon_creation=False
)
def stg_deals_extract_dag():
    dwh_pg_connect = PgConnect("PG_DWH_CONNECTION")

    @task(task_id="stg_extract_deals")
    def stg_extract_deals(logical_date=None):
        origin = DealsOriginRepository(URL)
        dest = DealsDestRepository()
        errors = ErrorsRepository()
        extractor = DealsExtractor(origin, dwh_pg_connect, dest, errors, log)

        count = extractor.extract_deals(logical_date)
        return f"stg_deals_extracted: {count} rows"

    extract = stg_extract_deals()

    dbt_run = BashOperator(
        task_id="dbt_build_dwh",
        bash_command="cd /opt/airflow/dbt && dbt build --target dev"
    )

    extract >> dbt_run


stg_deals_extract_dag = stg_deals_extract_dag()