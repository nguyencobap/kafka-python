import json
import ssl

from kafka import KafkaProducer
from kafka.errors import KafkaError

ssl_context = ssl._create_unverified_context(cafile='secrets/truststore/CARoot.pem')
ssl_context.load_cert_chain(certfile='secrets/truststore/ca-cert', keyfile='secrets/truststore/ca-key')
producer = KafkaProducer(
    bootstrap_servers=['acs.ncsgroup.vn:29092'],
    security_protocol='SSL',
    ssl_context=ssl_context,
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

while True:
    user_input = input()

    try:
        producer.send('my-topic', {'key': user_input})
    except KafkaError as ke:
        print("Kafka error: ", ke)

producer.flush()
