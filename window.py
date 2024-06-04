from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Define the schema for the data
schema = StructType([
    StructField("ts", TimestampType(), True),  
    StructField("price", FloatType(), True), 
    StructField("currency", StringType(), True), 
    StructField("volume", FloatType(), True)  
])

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkWindowing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the JSON data and apply the schema
value_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


value_df = kafka_df.selectExpr("CAST(transformed_data['currency'])", "CAST(json.dumps(transformed_data)))" \
    .select(from_json(col(transformed_data['currency']), schema).alias(data)) \
    .select(data)

# Apply windowing to the stream
windowed_df = value_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "5 seconds")
    ) \
    .agg(
        col("timestamp").alias("open_time"),
        col("price").alias("open"),
        col("price").alias("close"),
        col("price").max().alias("max"),
        col("price").min().alias("min")
    )

# Select the required columns and rename them
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("open"),
    col("close"),
    col("max"),
    col("min")
)

# Start the streaming query and print to console
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
