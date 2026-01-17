import logging

import pendulum
from airflow.sdk import dag, task
from airflow.models.variable import Variable
from lib.pg.ddl_runner import DdlRunner
from lib.pg.pg_connect import PgConnect

log = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['stg', 'schema', 'ddl'],
    is_paused_upon_creation=False
)
def stg_init_schema_dag():
    dwh_pg_connect = PgConnect("PG_DWH_CONNECTION")
    ddl_path = Variable.get("STG_DDL_FILES_PATH")

    @task(task_id="stg_schema_init")
    def schema_init():
        rest_loader = DdlRunner(dwh_pg_connect, log)
        rest_loader.run(ddl_path)
        return "stg_schema_initialized"

    init_schema = schema_init()

    init_schema


stg_init_schema_dag = stg_init_schema_dag()
