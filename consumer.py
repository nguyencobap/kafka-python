from kafka import KafkaConsumer
import ssl

ssl_context = ssl._create_unverified_context(cafile='secrets/truststore/CARoot.pem')
ssl_context.load_cert_chain(certfile='secrets/truststore/ca-cert', keyfile='secrets/truststore/ca-key')

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['acs.ncsgroup.vn:29093'],
    group_id='my-group',
    security_protocol='SSL',
    ssl_context=ssl_context,
    auto_offset_reset='earliest'
)

try:
    for message in consumer:
        print(message)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
