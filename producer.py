import json
import ssl

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import Config

config = Config()


def value_serializer(value_dict: dict):
    return json.dumps(value_dict).encode('utf-8')


if config.kafka_ssl_enable:
    ssl_context = ssl._create_unverified_context(cafile=config.kafka_ca_file)
    ssl_context.load_cert_chain(certfile=config.kafka_cert_file, keyfile=config.kafka_key_file)
    producer = KafkaProducer(
        bootstrap_servers=[config.kafka_bootstrap_server],
        value_serializer=value_serializer,
        security_protocol='SSL',
        ssl_context=ssl_context,
    )
else:
    producer = KafkaProducer(
        bootstrap_servers=[config.kafka_bootstrap_server],
        value_serializer=value_serializer,
    )

while True:
    user_input = input()

    try:
        producer.send(config.kafka_topic, {'x': user_input})
    except KafkaError as ke:
        print("Kafka error: ", ke)

producer.flush()
