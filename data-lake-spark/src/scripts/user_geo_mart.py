from utils.extract_events import get_events_nearest_geo
from pyspark.sql import SparkSession
import os
import sys
import pyspark.sql.functions as F
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

# Команда запуска на кластере:
# /usr/lib/spark/bin/spark-submit \
#   --master yarn \
#   --deploy-mode cluster \
#   user_geo_mart.py \
#   /user/nikkhav/project/geo/staging/events/ \
#   /user/nikkhav/data/geo/geo.csv \
#   /user/nikkhav/project/geo/cdm/user_geo_mart

def main():
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = (
        SparkSession.builder
        .appName("UserGeoMartJob")
        .getOrCreate()
    )
    events_nearest, geo = get_events_nearest_geo(spark, events_path, geo_path)

    w_user = Window.partitionBy("user_id").orderBy(F.col("event.message_ts").desc())

    user_act_city = (
        events_nearest
        .withColumn("rn", F.row_number().over(w_user))
        .filter(F.col("rn") == 1)
        .select(
            F.col("user_id"),
            F.col("city").alias("act_city"),
            "lat_city",
            "lon_city"
        )
    )

    # Добавляем в датафрейм дату события, предыдущий город и флаг смены города
    events_seq_base = (
        events_nearest
        .withColumn("event_date", F.to_date("event.message_ts"))
    )

    w_user_seq = Window.partitionBy("user_id").orderBy("event_date")
    events_seq_base = (
        events_seq_base
        .withColumn("prev_city", F.lag("city").over(w_user_seq))
        .withColumn("city_changed", F.when(F.col("city") != F.col("prev_city"), 1).otherwise(0))
        .withColumn("group_id", F.sum("city_changed").over(w_user_seq))
    )

    # Вычисляем периоды пребывания пользователя в каждом городе
    user_city_periods = (
        events_seq_base
        .groupBy("user_id", "city", "group_id")
        .agg(
            F.min("event_date").alias("start_date"),
            F.max("event_date").alias("end_date"),
            (F.datediff(F.max("event_date"), F.min("event_date")) + 1).alias("duration_days")
        )
    )

    # Оставляем только периоды длительностью не менее 27 дней
    long_stays = user_city_periods.filter(F.col("duration_days") >= 27)
    w_last = Window.partitionBy("user_id").orderBy(F.col("end_date").desc())

    # Определяем домашний город как последний по дате окончания длительного пребывания (более 27 дней)
    user_home_city = (
        long_stays
        .withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select(
            F.col("user_id"),
            F.col("city").alias("home_city")
        )
    )

    # Выбираем только события, где был сменен город
    travel_changes = (
        events_seq_base
        .filter(F.col("city_changed") == 1)
        .select("user_id", "event_date", "city")
        .orderBy("user_id", "event_date")
    )

    # Считаем количество поездок и собираем список посещенных городов
    travel_agg = (
        travel_changes
        .groupBy("user_id")
        .agg(
            F.count("city").alias("travel_count"),
            F.collect_list("city").alias("travel_array")
        )
    )

    geo_tz = geo.select(
        F.col("city").alias("geo_city"),
        F.col("timezone").alias("geo_timezone")
    )

    user_local_time = (
        events_nearest
        .join(geo_tz, events_nearest["city"] == geo_tz["geo_city"], "left")
        .withColumn("TIME_UTC", F.to_timestamp("event.message_ts"))
        .withColumn("local_time", F.from_utc_timestamp("TIME_UTC", F.col("geo_timezone")))
        .groupBy("user_id")
        .agg(F.max("local_time").alias("local_time"))
    )

    user_geo_mart = (
        user_act_city
        .join(user_home_city, "user_id", "left")
        .join(travel_agg, "user_id", "left")
        .join(user_local_time, "user_id", "left")
    )

    user_geo_mart.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    main()