from kafka import KafkaConsumer
import ssl
from config import Config

config = Config()
print(config)
if config.kafka_ssl_enable:
    ssl_context = ssl._create_unverified_context(cafile=config.kafka_ca_file)
    ssl_context.load_cert_chain(certfile=config.kafka_cert_file, keyfile=config.kafka_key_file)
    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_bootstrap_server,
        group_id='my-group1',
        auto_offset_reset='earliest',
        security_protocol='SSL',
        ssl_context=ssl_context,
    )
else:
    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_bootstrap_server,
        group_id='my-group',
        auto_offset_reset='earliest',
    )

try:
    for message in consumer:
        value = message.value
        print(value, type(value))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
