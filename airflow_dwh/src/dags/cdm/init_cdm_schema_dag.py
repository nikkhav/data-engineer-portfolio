import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.schema_init import SchemaDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *', # 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'schema', 'ddl'],
    is_paused_upon_creation=True
)
def cdm_init_schema_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("CDM_DDL_FILES_PATH")

    @task(task_id="cdm_schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    init_schema = schema_init()

    init_schema


cdm_init_schema_dag = cdm_init_schema_dag()
