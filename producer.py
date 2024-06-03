import logging
from confluent_kafka import Producer, KafkaException
import websocket
from datetime import datetime, timezone
import json
from time import sleep
from typing import Dict
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType

def setup_topic(topic: str, bootstrap_servers: str = "localhost:29092", num_partitions: int = 1, replication_factor: int = 1,
                retention_ms: int = None, compaction: bool = None, config: dict = None, num_attempts: int = 10) -> bool:

    client = AdminClient({"bootstrap.servers": bootstrap_servers})

    if not config: config = {}
    if compaction: config["cleanup.policy"] = "delete,compact"
    if retention_ms: config["retention.ms"] = str(retention_ms)
    if retention_ms and "segment.ms" not in config: config["segment.ms"] = str(retention_ms)

    # Perform multiple attempts to deal with eventual consistency and concurrent topic creation attempts
    last_ex = None
    for _ in range(num_attempts):
        # The topic may already exist: check if its configuration matches the desired one
        t = client.list_topics().topics.get(topic)
        if t is not None:
            np = len(t.partitions)
            rf = len(t.partitions[0].replicas) if np > 0 else 0
            if num_partitions == np and replication_factor == rf:
                logging.info(f"Found existing topic '{topic}' with {np} partitions and replication factor {rf}")
                c = ConfigResource(ResourceType.TOPIC, topic, config)
                client.alter_configs([c]).get(c).result() # enforce config / async operation, wait for completion
                return False # signal topic already exists
            else:
                client.delete_topics([topic]).get(topic).result()
                logging.info(f"Deleted existing topic '{topic}' with {np} partitions and replication factor {rf}")

        # Create the topic if it was not there or we deleted a conflicting one
        try:
            n = NewTopic(topic=topic, num_partitions=num_partitions, replication_factor=replication_factor, config=config)
            client.create_topics(new_topics=[n]).get(topic).result() # async operation, wait for completion
            logging.info(f"Created new topic '{topic}' with {num_partitions} partitions, replication factor {replication_factor}, config {config}")
            return True
        except KafkaException as ex:
            last_ex = ex
            sleep(1.0) # ignore, wait and retry, topic may be marked for deletion

    # Fail
    raise Exception(f"Cound not create topic {topic} after {num_attempts}: {last_ex}")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_bootstrap_servers = 'localhost:29092'
kafka_topic = 'trades'

# Set up Kafka topics
setup_topic(kafka_topic, kafka_bootstrap_servers)
#setup_topic(kafka_output_topic, kafka_bootstrap_servers)

# Finnhub WebSocket configuration
finnhub_ws_url = 'wss://ws.finnhub.io?token=cm239g9r01qvesfhfk70cm239g9r01qvesfhfk7g'

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'client.id': 'finnhub_producer'
}

# Kafka producer instance
producer = Producer(producer_config)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data and isinstance(data['data'], list):
            for trade_data in data['data']:
                transformed_data = {
                    'price': trade_data.get('p', 0),
                    'currency': trade_data.get('s', ''),
                    'ts': datetime.utcfromtimestamp(trade_data.get('t', 0) / 1000).isoformat(timespec='seconds'),
                    'volume': trade_data.get('v', 0)
                }
                k = transformed_data['currency']
                v = json.dumps(transformed_data)

                producer.produce(topic="input", key=k, value=v)

                producer.flush()
                logger.info(f"Produced message: {v}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")



def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket closed with status code: %s, message: %s", close_status_code, close_msg)

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(finnhub_ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()