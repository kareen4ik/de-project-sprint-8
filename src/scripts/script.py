import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, lit, struct, current_timestamp, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_NAME_IN = "student.topic.cohort32.yc-user_in"
TOPIC_NAME_OUT = "student.topic.cohort32.yc-user_out"

postgresql_settings = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "driver": "org.postgresql.Driver",
    "dbtable": "public.subscribers_feedback",
    "user": "student",
    "password": "de-student"
}

def foreach_batch_function(df, epoch_id):
    df.persist()

    df_with_feedback = df.withColumn("feedback", lit(None).cast("string"))

    df_with_feedback.write \
        .format("jdbc") \
        .mode("append") \
        .options(**postgresql_settings) \
        .save()

    df_for_kafka = df.select(
        "restaurant_id",
        "adv_campaign_id",
        "adv_campaign_content",
        "adv_campaign_owner",
        "adv_campaign_owner_contact",
        "adv_campaign_datetime_start",
        "adv_campaign_datetime_end",
        "datetime_created",
        "trigger_datetime_created",
        "client_id"
    )

    df_for_kafka_json = df_for_kafka.select(
        to_json(struct(col("*"))).alias("value")
    )

    df_for_kafka_json \
        .selectExpr("CAST(value AS STRING) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
        .option("topic", TOPIC_NAME_OUT) \
        .save()

    df.unpersist()


spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

restaurant_read_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option("subscribe", TOPIC_NAME_IN) \
    .load()

incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

deserialized_df = restaurant_read_stream_df \
    .selectExpr("CAST(value AS STRING) AS json_string") \
    .select(from_json(col("json_string"), incomming_message_schema).alias("json_data")) \
    .select("json_data.*")

with_current_ts_df = deserialized_df.withColumn(
    "current_ts",
    unix_timestamp(current_timestamp()).cast("long")
)

filtered_read_stream_df = with_current_ts_df.filter(
    (col("current_ts") >= col("adv_campaign_datetime_start")) &
    (col("current_ts") <= col("adv_campaign_datetime_end"))
)

subscribers_restaurant_df = spark.read \
    .format("jdbc") \
    .option("url", postgresql_settings["url"]) \
    .option("driver", postgresql_settings["driver"]) \
    .option("dbtable", "subscribers_restaurants") \
    .option("user", postgresql_settings["user"]) \
    .option("password", postgresql_settings["password"]) \
    .load()

joined_df = filtered_read_stream_df.join(
    subscribers_restaurant_df,
    on="restaurant_id",
    how="inner"
)

result_df = joined_df.withColumn(
    "trigger_datetime_created",
    unix_timestamp(current_timestamp()).cast("long")
)

query = result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()
