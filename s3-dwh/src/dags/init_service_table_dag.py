import logging
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.pg.ddl_runner import DdlRunner
from lib.pg.pg_connect import PgConnect

log = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    tags=['schema', 'ddl'],
    is_paused_upon_creation=False
)
def init_service_table_dag():
    dwh_pg_connect = PgConnect("PG_DWH_CONNECTION")

    current_dir = Path(__file__).parent
    ddl_path = current_dir / "service_ddl"

    @task(task_id="service_table_init")
    def service_table_init():
        ddl_runner = DdlRunner(dwh_pg_connect, log)
        ddl_runner.run(str(ddl_path))
        return "service_table_initialized"

    init_service = service_table_init()

    init_service


init_service_table_dag = init_service_table_dag()
