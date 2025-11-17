from pyspark.sql import SparkSession
import os
import sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

# Создание необходимых директорий и загрузка файлов (выполнить один раз на кластере):
# !hdfs dfs -mkdir -p /user/nikkhav/data/geo
# !hdfs dfs -copyFromLocal geo.csv /user/nikkhav/data/geo/
# !hdfs dfs -mkdir -p /user/nikkhav/project/geo/{staging,dds,cdm}

# Команда запуска на кластере:
# /usr/lib/spark/bin/spark-submit \
#   --master yarn \
#   --deploy-mode cluster \
#   data_overwrite.py \
#   /user/master/data/geo/events \
#   /user/nikkhav/data/geo/geo.csv \
#   /user/nikkhav/project/geo/staging/events/ \
#   /user/nikkhav/project/geo/staging/geo/


def main():
    events_input_path = sys.argv[1]
    geo_input_path = sys.argv[2]
    events_output_path = sys.argv[3]
    geo_output_path = sys.argv[4]

    spark = (
            SparkSession.builder
            .appName(f"DataOverwriteJob")
            .getOrCreate()
        )

    events = spark.read.parquet(events_input_path)
    geo = spark.read.csv(geo_input_path, header=True, inferSchema=True)

    events.write.mode("overwrite").parquet(events_output_path)
    geo.write.mode("overwrite").parquet(geo_output_path)

if __name__ == "__main__":
    main()