import os
from threading import Lock


class ConfigMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Config(metaclass=ConfigMeta):
    kafka_bootstrap_server: str
    kafka_topic: str
    kafka_ssl_enable: bool
    kafka_ca_file: str
    kafka_cert_file: str
    kafka_key_file: str

    def __init__(self):
        if "KAFKA_BOOTSTRAP_SERVER" in os.environ:
            self.kafka_bootstrap_server = os.environ["KAFKA_BOOTSTRAP_SERVER"].split(",")
        if "KAFKA_TOPIC" in os.environ:
            self.kafka_topic = os.environ["KAFKA_TOPIC"]
        if "KAFKA_SSL_ENABLE" in os.environ:
            self.kafka_ssl_enable = os.environ["KAFKA_SSL_ENABLE"].lower() == "true"
        if "KAFKA_CA_FILE" in os.environ:
            self.kafka_ca_file = os.environ["KAFKA_CA_FILE"]
        if "KAFKA_CERT_FILE" in os.environ:
            self.kafka_cert_file = os.environ["KAFKA_CERT_FILE"]
        if "KAFKA_KEY_FILE" in os.environ:
            self.kafka_key_file = os.environ["KAFKA_KEY_FILE"]

    def __str__(self):
        attributes = vars(self)
        attribute_strings = [f"{key}: {value}" for key, value in attributes.items()]
        return ", ".join(attribute_strings)
