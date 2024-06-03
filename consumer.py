from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from kafka import KafkaConsumer
import json

def create_consumer():
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='your_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def consume_messages(consumer):
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
            # Add your custom processing logic here
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

# Define the schema for the data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("price", FloatType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkWindowing") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your_topic_name") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the JSON data and apply the schema
value_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply windowing to the stream
windowed_df = value_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "5 seconds")
    ) \
    .agg(
        col("timestamp").alias("open_time"),
        col("price").alias("open_price"),
        col("price").alias("close_price"),
        col("price").max().alias("max_price"),
        col("price").min().alias("min_price")
    )

# Select the required columns and rename them
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("open_price"),
    col("close_price"),
    col("max_price"),
    col("min_price")
)

# Start the streaming query and print to console
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

if __name__ == "__main__":
    consumer = create_consumer()
    consume_messages(consumer)
