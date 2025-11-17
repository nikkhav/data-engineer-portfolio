import logging

import pendulum
from airflow.decorators import dag, task
from cdm.dm_settlement_report.dm_settlement_report_loader import ReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'dm_settlement_report'],
    is_paused_upon_creation=True
)
def dm_settlement_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id='load_dm_settlement_report')
    def load_dm_settlement_report():
        rest_loader = ReportLoader(dwh_pg_connect, log)
        rest_loader.load_reports()
        return "reports_loaded"

    load_dm_settlement_report = load_dm_settlement_report()

    load_dm_settlement_report

dm_settlement_report_dag = dm_settlement_report_dag()
