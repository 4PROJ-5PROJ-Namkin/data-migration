import json
import logging
from kafka import KafkaConsumer

class KafkaConsumerClient:
    """
    A Kafka consumer client that subscribes to a specified topic and consumes messages.

    """

    def __init__(self, servers, topic, group_id=None):
        """
        Initializes the KafkaConsumerClient.

        :param servers: List of bootstrap servers in the format ['host1:port', 'host2:port', ...]
        :param topic: The topic to subscribe to.
        :param group_id: The group ID to use for consuming messages. If None, a random group ID is assigned.

        """
        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            auto_offset_reset='earliest',
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.topic = topic
        self.configure_logging()

    def configure_logging(self):
        """Configures the logging settings for the consumer."""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def subscribe(self):
        """Subscribes the consumer to the topic."""
        self.consumer.subscribe([self.topic])
        self.logger.info(f"Subscribed to topic: {self.topic}")

    def consume_messages(self):
        """Consumes and processes messages from the subscribed topic."""
        try:
            self.logger.info(f"Starting to consume messages from {self.topic}")
            for message in self.consumer:
                self.logger.debug(f"Message received from partition {message.partition}")
                print(f"Message received from partition {message.partition} at offset {message.offset} : {message.value}")
        except Exception as e:
            self.logger.error(f"An error occurred while consuming messages: {e}")
        finally:
            self.close()

    def close(self):
        """Closes the Kafka consumer."""
        self.logger.info(f"Closing the consumer for topic: {self.topic}")
        self.consumer.close()

if __name__ == "__main__":
    kafka_servers = ['localhost:9092']
    topic_name = 'materials'
    group_id = 'mon_nouveau_group_id'

    consumer_client = KafkaConsumerClient(servers=kafka_servers, topic=topic_name, group_id=group_id)
    consumer_client.subscribe()
    consumer_client.consume_messages()
