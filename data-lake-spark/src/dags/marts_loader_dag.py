import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# ==== Конфигурация окружения ====
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

# ==== Аргументы DAG ====
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="marts_loader_dag",
    default_args=default_args,
    schedule_interval=None,
)

data_overwrite = SparkSubmitOperator(
    task_id='data_overwrite',
    dag=dag_spark,
    application='data_overwrite.py',
    conn_id='yarn_spark',
    application_args=[
      "/user/master/data/geo/events",
      "/user/nikkhav/data/geo/geo.csv",
      "/user/nikkhav/project/geo/staging/events",
      "/user/nikkhav/project/geo/staging/geo",
    ]
)

user_geo_mart = SparkSubmitOperator(
    task_id='user_geo_mart',
    dag=dag_spark,
    application='user_geo_mart.py',
    conn_id='yarn_spark',
    application_args=[
      "/user/nikkhav/project/geo/staging/events",
      "/user/nikkhav/data/geo/geo.csv",
      "/user/nikkhav/project/geo/cdm/user_geo_mart"
    ]
)

zone_geo_mart = SparkSubmitOperator(
    task_id='zone_geo_mart',
    dag=dag_spark,
    application='zone_geo_mart.py',
    conn_id='yarn_spark',
    application_args=[
      "/user/nikkhav/project/geo/staging/events",
      "/user/nikkhav/data/geo/geo.csv",
      "/user/nikkhav/project/geo/cdm/zone_geo_mart"
    ]
)

friend_recommendations_mart = SparkSubmitOperator(
    task_id='friend_recommendations_mart',
    dag=dag_spark,
    application='friend_recommendations_mart.py',
    conn_id='yarn_spark',
    application_args=[
      "/user/nikkhav/project/geo/staging/events",
      "/user/nikkhav/data/geo/geo.csv",
      "/user/nikkhav/project/geo/cdm/friend_recommendations"
    ]
)

data_overwrite >> user_geo_mart >> zone_geo_mart >> friend_recommendations_mart
