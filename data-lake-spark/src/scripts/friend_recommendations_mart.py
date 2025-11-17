from utils.extract_events import get_events_nearest_geo
from utils.calc_distance import calc_distance
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
#   friend_recommendations_mart.py \
#   /user/nikkhav/project/geo/staging/events/ \
#   /user/nikkhav/data/geo/geo.csv \
#   /user/nikkhav/project/geo/cdm/friend_recommendations

def main():
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = (
        SparkSession.builder
        .appName("FriendsRecommendationsMartJob")
        .getOrCreate()
    )
    events_nearest, geo = get_events_nearest_geo(spark, events_path, geo_path)

    subscriptions = (
        events_nearest
        .filter(F.col("event_type") == "subscription")
        .select(
            F.col("event.subscription_user").alias("user_id"),
            F.col("event.subscription_channel").alias("channel_id")
        )
        .dropna()
        .dropDuplicates()
    )

    pairs = (
        subscriptions.alias("a")
        .join(
            subscriptions.alias("b"),
            (F.col("a.channel_id") == F.col("b.channel_id")) &
            (F.col("a.user_id") < F.col("b.user_id")),
            "inner"
        )
        .select(
            F.col("a.user_id").alias("user_left"),
            F.col("b.user_id").alias("user_right"),
            F.col("a.channel_id")
        )
    )

    messages = (
        events_nearest
        .filter(F.col("event_type") == "message")
        .select(
            F.col("event.message_from").alias("user_from"),
            F.col("event.message_to").alias("user_to")
        )
        .dropna()
        .dropDuplicates()
    )

    pairs = pairs.join(
        messages,
        (
                ((F.col("user_left").cast("long") == F.col("user_from")) &
                 (F.col("user_right").cast("long") == F.col("user_to"))) |
                ((F.col("user_left").cast("long") == F.col("user_to")) &
                 (F.col("user_right").cast("long") == F.col("user_from")))
        ),
        "left_anti"
    )

    # Отбираем последние события для каждого пользователя
    w_last = Window.partitionBy("user_id").orderBy(F.col("event.message_ts").desc())

    # Для самых новых событий берем координаты и зону
    last_locations = (
        events_nearest
        .withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select(
            F.col("user_id"),
            F.col("lat").alias("lat_current"),
            F.col("lon").alias("lon_current"),
            F.col("id_city").alias("zone_id"),
            F.col("city")
        )
    )

    # Соединяем пары с их последними локациями
    pairs = (
        pairs
        .join(last_locations.alias("left_loc"), pairs.user_left == F.col("left_loc.user_id"), "left")
        .join(last_locations.alias("right_loc"), pairs.user_right == F.col("right_loc.user_id"), "left")
    )

    # Для выбранных пар вычисляем расстояние между пользователями и фильтруем по расстоянию <= 1 км
    pairs = pairs.withColumn(
        "distance_km",
        calc_distance(
            F.col("left_loc.lat_current"),
            F.col("left_loc.lon_current"),
            F.col("right_loc.lat_current"),
            F.col("right_loc.lon_current")
        )
    ).filter(F.col("distance_km") <= 1)

    pairs = (
        pairs
        .join(
            geo.select(F.col("city").alias("geo_city"), F.col("timezone")),
            F.col("left_loc.city") == F.col("geo_city"),
            "left"
        )
        .drop("geo_city")
    )

    pairs = pairs.withColumn("processed_dttm", F.current_timestamp())
    pairs = pairs.withColumn("local_time", F.from_utc_timestamp(F.current_timestamp(), F.col("timezone")))


    friend_recommendations = pairs.select(
        "user_left",
        "user_right",
        F.col("left_loc.zone_id").alias("zone_id"),
        "local_time",
        "processed_dttm"
    )

    friend_recommendations.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    main()
