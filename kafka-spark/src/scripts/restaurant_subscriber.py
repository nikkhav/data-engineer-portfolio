import os
import time
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, unix_timestamp,
    sha2, concat_ws, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC_NAME_IN = os.getenv("KAFKA_TOPIC_IN")
TOPIC_NAME_OUT = os.getenv("KAFKA_TOPIC_OUT")
TOPIC_NAME_DLQ = os.getenv("KAFKA_TOPIC_DLQ")
KAFKA_TRANSACTIONAL_ID = os.getenv("KAFKA_TRANSACTIONAL_ID")

PG_JDBC_URL_READ = os.getenv("PG_JDBC_URL_READ")
PG_USER_READ = os.getenv("PG_USER_READ")
PG_PASS_READ = os.getenv("PG_PASS_READ")

PG_JDBC_URL_WRITE = os.getenv("PG_JDBC_URL_WRITE")
PG_USER_WRITE = os.getenv("PG_USER_WRITE")
PG_PASS_WRITE = os.getenv("PG_PASS_WRITE")
PG_TARGET_TABLE = os.getenv("PG_TARGET_TABLE")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR")
MAX_BATCH_ROWS = int(os.getenv("MAX_BATCH_ROWS", "50000"))

# --- Kafka security ---
kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config":
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
    "kafka.isolation.level": "read_committed"
}

# --- Spark setup ---
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

spark = (
    SparkSession.builder
    .appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Schemas ---
incoming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

# --- Source stream ---
restaurant_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .options(**kafka_security_options)
    .option("subscribe", TOPIC_NAME_IN)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    restaurant_stream
    .select(
        from_json(col("value").cast("string"), incoming_message_schema).alias("data"),
        col("value").alias("raw_value"),
        col("offset")
    )
)

good = parsed.where(col("data").isNotNull()).select("data.*", "offset")
bad = parsed.where(col("data").isNull()).select(
    col("raw_value").cast("string").alias("raw_value"),
    lit("json_error").alias("reason")
)

# --- Timestamp filter ---
now_utc = unix_timestamp(current_timestamp())
filtered = good.filter(
    (col("adv_campaign_datetime_start") <= now_utc) &
    (col("adv_campaign_datetime_end") >= now_utc)
)

# --- Subscribers from PG ---
subscribers = (
    spark.read
    .format("jdbc")
    .option("url", PG_JDBC_URL_READ)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "subscribers_restaurants")
    .option("user", PG_USER_READ)
    .option("password", PG_PASS_READ)
    .load()
    .select("client_id", "restaurant_id")
)

# --- Join ---
joined = (
    filtered.alias("k")
    .join(subscribers.alias("s"), "restaurant_id")
    .select(
        "k.restaurant_id", "adv_campaign_id", "adv_campaign_content",
        "adv_campaign_owner", "adv_campaign_owner_contact",
        "adv_campaign_datetime_start", "adv_campaign_datetime_end",
        "datetime_created", "s.client_id"
    )
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()).cast("int"))
)

# --- event_id + dedup ---
result = (
    joined.withColumn(
        "event_id",
        sha2(concat_ws("||", "restaurant_id", "adv_campaign_id", "client_id"), 256)
    )
    .withColumn("event_ts", col("datetime_created").cast("timestamp"))
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id"])
)

# --- Write to Postgres ---
def write_to_pg(df: DataFrame):
    rows = df.collect()
    if not rows:
        return
    conn = psycopg2.connect(
        host=PG_JDBC_URL_WRITE.split("//")[1].split("/")[0].split(":")[0],
        port=int(PG_JDBC_URL_WRITE.split("//")[1].split("/")[0].split(":")[1]),
        dbname=PG_JDBC_URL_WRITE.split("/")[-1],
        user=PG_USER_WRITE,
        password=PG_PASS_WRITE
    )
    conn.autocommit = False
    cur = conn.cursor()
    sql = f"""
    INSERT INTO {PG_TARGET_TABLE}
    (event_id, restaurant_id, adv_campaign_id, adv_campaign_content,
     adv_campaign_owner, adv_campaign_owner_contact,
     adv_campaign_datetime_start, adv_campaign_datetime_end,
     datetime_created, client_id, trigger_datetime_created)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (event_id) DO NOTHING;
    """
    data = [
        (
            r["event_id"], r["restaurant_id"], r["adv_campaign_id"],
            r["adv_campaign_content"], r["adv_campaign_owner"],
            r["adv_campaign_owner_contact"], r["adv_campaign_datetime_start"],
            r["adv_campaign_datetime_end"], r["datetime_created"],
            r["client_id"], r["trigger_datetime_created"]
        ) for r in rows
    ]
    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

# --- foreachBatch handler ---
def foreach_batch(df, epoch):
    cnt = df.count()
    print(f"[INFO] Epoch {epoch}: {cnt} rows")
    if cnt > MAX_BATCH_ROWS:
        bad_df = df.select(to_json(struct(*df.columns)).alias("value"))
        bad_df.write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .options(**kafka_security_options) \
            .option("topic", TOPIC_NAME_DLQ) \
            .save()
        return
    write_to_pg(df)
    kafka_df = df.select(to_json(struct(*df.columns)).alias("value"))
    kafka_df.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .options(**kafka_security_options) \
        .option("kafka.transactional.id", KAFKA_TRANSACTIONAL_ID) \
        .option("topic", TOPIC_NAME_OUT) \
        .save()

# --- Write streams ---
bad.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .options(**kafka_security_options) \
    .option("topic", TOPIC_NAME_DLQ) \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "dlq")) \
    .start()

result.writeStream.foreachBatch(foreach_batch) \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "main")) \
    .start() \
    .awaitTermination()
