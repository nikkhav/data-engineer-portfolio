import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from stg.order_system_dag.loaders.orders_loader import (
    OrdersOriginRepository,
    OrdersDestRepository,
    OrderLoader,
)
from stg.order_system_dag.loaders.restaurants_loader import (
    RestaurantsOriginRepository,
    RestaurantsDestRepository,
    RestaurantLoader,
)
from stg.order_system_dag.loaders.users_loader import (
    UsersOriginRepository,
    UsersDestRepository,
    UserLoader,
)

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin", "mongo"],
    is_paused_upon_creation=True,
)
def stg_order_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    def build_mongo() -> MongoConnect:
        return MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

    @task(task_id="load_restaurants")
    def load_restaurants():
        origin = RestaurantsOriginRepository(build_mongo())
        dest = RestaurantsDestRepository()
        loader = RestaurantLoader(origin, dwh_pg_connect, dest, log)
        loader.load_restaurants()

    @task(task_id="load_users")
    def load_users():
        origin = UsersOriginRepository(build_mongo())
        dest = UsersDestRepository()
        loader = UserLoader(origin, dwh_pg_connect, dest, log)
        loader.load_users()

    @task(task_id="load_orders")
    def load_orders():
        origin = OrdersOriginRepository(build_mongo())
        dest = OrdersDestRepository()
        loader = OrderLoader(origin, dwh_pg_connect, dest, log)
        loader.load_orders()

    load_restaurants = load_restaurants()
    load_users = load_users()
    load_orders = load_orders()

    load_restaurants >> load_users >> load_orders


order_stg_dag = stg_order_system()
