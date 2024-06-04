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

if __name__ == "__main__":
    consumer = create_consumer()
    consume_messages(consumer)
