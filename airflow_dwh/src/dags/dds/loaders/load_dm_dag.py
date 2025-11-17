import logging

import pendulum
from airflow.decorators import dag, task

from dds.loaders.dm_couriers_loader import CourierLoader
from dds.loaders.dm_users_loader import UserLoader
from dds.loaders.dm_restaurants_loader import RestaurantLoader
from dds.loaders.dm_timestamps_loader import TimestampLoader
from dds.loaders.dm_products_loader import ProductLoader
from dds.loaders.dm_orders_loader import OrderLoader
from dds.loaders.fct_deliveries_loader import DeliveriesLoader
from dds.loaders.fct_product_sales_loader import ProductSalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'dm'],
    is_paused_upon_creation=True
)
def load_dm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id='load_dm_users')
    def load_dm_users():
        rest_loader = UserLoader(dwh_pg_connect, log)
        rest_loader.load_users()
        return "users_loaded"

    load_dm_users = load_dm_users()

    @task(task_id='load_dm_restaurants')
    def load_dm_restaurants():
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()
        return "restaurants_loaded"

    load_dm_restaurants = load_dm_restaurants()

    @task(task_id='load_dm_timestamps')
    def load_dm_timestamps():
        rest_loader = TimestampLoader(dwh_pg_connect, log)
        rest_loader.load_timestamps()
        return "timestamps_loaded"

    load_dm_timestamps = load_dm_timestamps()

    @task(task_id='load_dm_products')
    def load_dm_products():
        rest_loader = ProductLoader(dwh_pg_connect, log)
        rest_loader.load_products()
        return "products_loaded"

    load_dm_products = load_dm_products()

    @task(task_id='load_dm_orders')
    def load_dm_orders():
        order_loader = OrderLoader(dwh_pg_connect, log)
        order_loader.load_orders()
        return "orders_loaded"

    load_dm_orders = load_dm_orders()

    @task(task_id='load_dm_couriers')
    def load_dm_couriers():
        rest_loader = CourierLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()
        return "couriers_loaded"

    load_dm_couriers = load_dm_couriers()

    @task(task_id='load_fct_product_sales')
    def load_fct_product_sales():
        product_sales_loader = ProductSalesLoader(dwh_pg_connect, log)
        product_sales_loader.load_orders()
        return "product_sales_loaded"

    load_fct_product_sales = load_fct_product_sales()

    @task(task_id='load_fct_deliveries')
    def load_fct_deliveries():
        product_sales_loader = DeliveriesLoader(dwh_pg_connect, log)
        product_sales_loader.load_deliveries()
        return "deliveries_loaded"

    load_fct_deliveries = load_fct_deliveries()

    (load_dm_users >> load_dm_restaurants >> load_dm_timestamps >>
     load_dm_products >> load_dm_orders >> load_dm_couriers >>
     load_fct_product_sales >> load_fct_deliveries)

load_dm_dag = load_dm_dag()
