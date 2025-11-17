import logging

import pendulum
from airflow.decorators import dag, task
from cdm.dm_courier_ledger.dm_courier_ledger_loader import LedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval="0 0 * * 0", # Каждое воскресенье в полночь
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'dm_courier_ledger'],
    is_paused_upon_creation=True
)
def dm_courier_ledger_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id='load_dm_courier_ledger')
    def load_dm_courier_ledger():
        rest_loader = LedgerLoader(dwh_pg_connect, log)
        rest_loader.load_ledgers()
        return "ledgers_loaded"

    load_dm_courier_ledger = load_dm_courier_ledger()

    load_dm_courier_ledger

dm_courier_ledger_dag = dm_courier_ledger_dag()
