from typing import List
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import ssl
from config import Config

config = Config()
print(config)
if config.kafka_ssl_enable:
    ssl_context = ssl._create_unverified_context(cafile=config.kafka_ca_file)
    ssl_context.load_cert_chain(certfile=config.kafka_cert_file, keyfile=config.kafka_key_file)
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.kafka_bootstrap_server,
        security_protocol='SSL',
        ssl_context=ssl_context,
    )
else:
    admin_client = KafkaAdminClient(bootstrap_servers=config.kafka_bootstrap_server)

def create_topics(admin_client: KafkaAdminClient, list_topic_name: List[str], num_partitions: int=1, replication_factor: int=1):
    new_topics = []
    for topic_name in list_topic_name:
        topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        new_topics.append(topic)
    try:
        admin_client.create_topics(new_topics=new_topics)
        print(f"{list_topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Some topics already exists.")


def get_all_topics(admin_client: KafkaAdminClient):
    topics = admin_client.list_topics()
    return topics

def get_topics_metadata_by_name(admin_client: KafkaAdminClient, list_topic_name: List[str]) -> List[dict]:
    list_topic_metadata: List[dict] = admin_client.describe_topics(topics=list_topic_name)
    return list_topic_metadata

def delete_topic(admin_client: KafkaAdminClient, list_topic_name: List[str]):
    admin_client.delete_topics(topics=list_topic_name, timeout_ms=10000)
    print(f"Đã xóa chủ đề '{list_topic_name}' thành công.")

list_topic_name = get_all_topics(admin_client=admin_client)
list_topic_metadata = get_topics_metadata_by_name(admin_client=admin_client, list_topic_name=list_topic_name)
for topic_metadata in list_topic_metadata:
    print(topic_metadata)

