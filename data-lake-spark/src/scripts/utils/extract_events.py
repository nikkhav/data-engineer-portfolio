from pyspark.sql import functions as F, Window
from calc_distance import calc_distance

def get_events_nearest_geo(spark, events_path, geo_path):
    events = (
        spark.read.parquet(events_path)
        .withColumn("row_uid", F.monotonically_increasing_id())
    )

    geo = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(geo_path)
    )

    geo = (
        geo.withColumnRenamed("lat", "lat_city")
           .withColumnRenamed("lng", "lon_city")
           .withColumnRenamed("id", "id_city")
    )

    geo = geo.withColumn(
        "timezone",
        F.when(F.col("city").isin("Sydney", "Canberra", "Newcastle", "Wollongong", "Maitland"),
               F.lit("Australia/Sydney"))
        .when(F.col("city").isin("Melbourne", "Geelong", "Ballarat", "Bendigo"),
               F.lit("Australia/Melbourne"))
        .when(F.col("city").isin(
            "Brisbane", "Gold Coast", "Sunshine Coast", "Toowoomba", "Cairns",
            "Townsville", "Rockhampton", "Mackay", "Ipswich"),
               F.lit("Australia/Brisbane"))
        .when(F.col("city").isin("Adelaide"), F.lit("Australia/Adelaide"))
        .when(F.col("city").isin("Darwin"), F.lit("Australia/Darwin"))
        .when(F.col("city").isin("Perth", "Bunbury"), F.lit("Australia/Perth"))
        .when(F.col("city").isin("Hobart", "Launceston"), F.lit("Australia/Hobart"))
        .otherwise(F.lit("Australia/Sydney"))
    )

    events_geo = events.crossJoin(geo)
    events_geo = events_geo.withColumn(
        "distance",
        calc_distance(F.col("lat"), F.col("lon"), F.col("lat_city"), F.col("lon_city"))
    )

    w_event = Window.partitionBy("row_uid").orderBy(F.col("distance"))
    events_nearest = (
        events_geo
        .withColumn("rn", F.row_number().over(w_event))
        .filter(F.col("rn") == 1)
        .drop("rn", "distance")
    )

    events_nearest = events_nearest.withColumn(
        "user_id",
        F.coalesce(
            F.col("event.user"),
            F.col("event.message_from"),
            F.col("event.reaction_from"),
            F.col("event.subscription_user")
        )
    )

    return events_nearest, geo
