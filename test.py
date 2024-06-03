import logging
from time import sleep
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from threading import Thread
import read_data
import json

def setup_topic(topic: str, bootstrap_servers: str = "localhost:29092", num_partitions: int = 1, replication_factor: int = 1, retention_ms: int = None, compaction: bool = None, config: dict = None, num_attempts: int = 10) -> bool:
    client = AdminClient({"bootstrap.servers": bootstrap_servers})

    if not config: config = {}
    if compaction: config["cleanup.policy"] = "delete,compact"
    if retention_ms: config["retention.ms"] = str(retention_ms)
    if retention_ms and "segment.ms" not in config: config["segment.ms"] = str(retention_ms)

    last_ex = None
    for _ in range(num_attempts):
        t = client.list_topics().topics.get(topic)
        if t is not None:
            np = len(t.partitions)
            rf = len(t.partitions[0].replicas) if np > 0 else 0
            if num_partitions == np and replication_factor == rf:
                logging.info(f"Found existing topic '{topic}' with {np} partitions and replication factor {rf}")
                c = ConfigResource(ResourceType.TOPIC, topic, config)
                client.alter_configs([c]).get(c).result()
                return False
            else:
                client.delete_topics([topic]).get(topic).result()
                logging.info(f"Deleted existing topic '{topic}' with {np} partitions and replication factor {rf}")

        try:
            n = NewTopic(topic=topic, num_partitions=num_partitions, replication_factor=replication_factor, config=config)
            client.create_topics(new_topics=[n]).get(topic).result()
            logging.info(f"Created new topic '{topic}' with {num_partitions} partitions, replication factor {replication_factor}, config {config}")
            return True
        except KafkaException as ex:
            last_ex = ex
            sleep(1.0)

    raise Exception(f"Could not create topic {topic} after {num_attempts}: {last_ex}")

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Start the websocket data fetching in a separate thread
    websocket_thread = Thread(target=read_data.run_websocket)
    websocket_thread.start()

    # Kafka server information and topic setup
    kafka_bootstrap_servers = 'localhost:29092'
    kafka_topic = 'trades'

    # Set up Kafka topics
    setup_topic(kafka_topic, kafka_bootstrap_servers)

    # Set up Kafka Producer
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    # Define a delivery callback
    def delivery_callback(err, msg):
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    # Set up Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    # Define schema for Kafka data
    schema = StructType([
        StructField("price", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("volume", StringType(), True)
    ])

    # Process Kafka data
    # Converts the Kafka message value from binary to string
    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Send Kafka data to topic
    def send_to_kafka(row):
        message = row.asDict()
        producer.produce(kafka_topic, key=None, value=json.dumps(message), callback=delivery_callback)
        producer.poll(0)  # Trigger message delivery
        return

    query = df.writeStream \
        .foreach(send_to_kafka) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
