import logging
import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_dag.loaders.ranks_loader import RankLoader
from stg.bonus_system_dag.loaders.users_loader import UserLoader
from stg.bonus_system_dag.loaders.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2025, 9, 21, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def stg_bonus_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_ranks()
        return "ranks_loaded"

    @task(task_id="users_load")
    def load_users():
        loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_users()
        return "users_loaded"

    @task(task_id="events_load")
    def load_events():
        loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_events()
        return "events_loaded"

    load_ranks = load_ranks()
    load_users = load_users()
    load_events = load_events()

    load_ranks >> load_users >> load_events


stg_bonus_system_dag = stg_bonus_system_dag()
