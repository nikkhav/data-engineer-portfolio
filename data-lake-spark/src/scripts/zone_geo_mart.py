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
#   zone_geo_mart.py \
#   /user/nikkhav/project/geo/staging/events/ \
#   /user/nikkhav/data/geo/geo.csv \
#   /user/nikkhav/project/geo/cdm/zone_geo_mart

def main():
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = (
        SparkSession.builder
        .appName("ZoneGeoMartJob")
        .getOrCreate()
    )
    events_nearest, geo = get_events_nearest_geo(spark, events_path, geo_path)

    events_geo_time = (
        events_nearest
        .withColumn("event_date", F.to_date("event.message_ts"))
        .withColumn("month", F.date_format("event_date", "yyyy-MM"))
        .withColumn("week", F.weekofyear("event_date"))
    )

    w_first_event = Window.partitionBy("user_id").orderBy("event.message_ts")

    events_geo_time = (
        events_geo_time
        .withColumn("rn_user", F.row_number().over(w_first_event))
        .withColumn(
            "event_label",
            F.when(F.col("rn_user") == 1, "user")  # Первое событие — регистрация
            .when(F.col("event_type") == "message", "message")
            .when(F.col("event_type") == "reaction", "reaction")
            .when(F.col("event_type") == "subscription", "subscription")
            .otherwise("other")
        )
        .drop("rn_user")
    )

    zone_week = (
        events_geo_time
        .groupBy("id_city", "city", "week")
        .pivot("event_label", ["message", "reaction", "subscription", "user"])
        .agg(F.count("*"))
        .na.fill(0)
        .withColumnRenamed("message", "week_message")
        .withColumnRenamed("reaction", "week_reaction")
        .withColumnRenamed("subscription", "week_subscription")
        .withColumnRenamed("user", "week_user")
    )

    zone_month = (
        events_geo_time
        .groupBy("id_city", "city", "month")
        .pivot("event_label", ["message", "reaction", "subscription", "user"])
        .agg(F.count("*"))
        .na.fill(0)
        .withColumnRenamed("message", "month_message")
        .withColumnRenamed("reaction", "month_reaction")
        .withColumnRenamed("subscription", "month_subscription")
        .withColumnRenamed("user", "month_user")
    )

    zone_week = zone_week.withColumn("period_type", F.lit("week"))
    zone_month = zone_month.withColumn("period_type", F.lit("month"))

    zone_geo_mart = zone_week.selectExpr(
        "id_city as zone_id", "city", "week as period", "period_type",
        "week_message as message", "week_reaction as reaction",
        "week_subscription as subscription", "week_user as user"
    ).unionByName(
        zone_month.selectExpr(
            "id_city as zone_id", "city", "month as period", "period_type",
            "month_message as message", "month_reaction as reaction",
            "month_subscription as subscription", "month_user as user"
        )
    )

    zone_geo_mart.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    main()
