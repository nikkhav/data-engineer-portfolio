import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from pydantic import BaseModel

from stg.delivery_system_dag.loaders.couriers_loader import CourierLoader, CouriersOriginRepository, \
    CouriersDestRepository
from stg.delivery_system_dag.loaders.deliveries_loader import DeliveryLoader, DeliveriesOriginRepository, \
    DeliveriesDestRepository

from lib import ConnectionBuilder

log = logging.getLogger(__name__)

class ApiCredentials(BaseModel):
    host: str
    api_key: str
    cohort: str
    nickname: str


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def stg_delivery_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    conn = BaseHook.get_connection("delivery_system_api")
    host = conn.host
    api_key = conn.extra_dejson["api_key"]
    cohort = conn.extra_dejson["cohort"]
    nickname = conn.extra_dejson["nickname"]

    api_credentials = ApiCredentials(
        host=host,
        api_key=api_key,
        cohort=cohort,
        nickname=nickname,
    )

    @task(task_id="load_couriers")
    def load_couriers():
        origin = CouriersOriginRepository(api_credentials)
        dest = CouriersDestRepository()
        rest_loader = CourierLoader(origin, dwh_pg_connect, dest, log)
        rest_loader.load_couriers()
        return "couriers_loaded"

    @task(task_id="load_deliveries")
    def load_deliveries():
        origin = DeliveriesOriginRepository(api_credentials)
        dest = DeliveriesDestRepository()
        rest_loader = DeliveryLoader(origin, dwh_pg_connect, dest, log)
        rest_loader.load_deliveries()
        return "deliveries_loaded"


    load_couriers = load_couriers()
    load_deliveries = load_deliveries()

    load_couriers >> load_deliveries


delivery_stg_dag = stg_delivery_system()
