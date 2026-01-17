import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

log = logging.getLogger(__name__)

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=True,
    tags=['dwh', 'sql'],
    is_paused_upon_creation=False
)
def dwh_global_metrics_dag():
    ingest_global_metrics = SQLExecuteQueryOperator(
        task_id="ingest_global_metrics",
        conn_id="VERTICA_DWH_CONNECTION",
        sql="dwh/global_metrics_increment.sql",
        parameters={
            "target_date": "{{ macros.ds_add(ds, -1) }}"
        }
    )

    ingest_global_metrics


dwh_global_metrics_dag = dwh_global_metrics_dag()
