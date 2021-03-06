import json
from time import sleep

from kafka import KafkaConsumer
from datalake.settings import KAFKA_HOST, KAFKA_PORT

from kafka_adapter.handler import KafkaBaseHandler
import logging

from kafka_adapter.utils import MetaSingleton


class KafkaAdapter(metaclass=MetaSingleton):
    topics = []
    handlers = []
    consumer = None

    @classmethod
    def get_available_kafka_handlers(cls):
        return KafkaBaseHandler.__subclasses__()

    @classmethod
    def get_appropriate_handlers(cls, kafka_topic):
        res = []
        for h in cls.handlers:
            if h.get_topic() == kafka_topic:
                res.append(h)
        return res

    @classmethod
    def start(cls):
        cls.handlers = cls.get_available_kafka_handlers()
        logging.info(f"Available handlers: {cls.handlers}")
        cls.topics = [h.get_topic() for h in cls.handlers]
        while not cls.topics:
            logging.info(f"No topics to monitor: {cls.topics}")
            sleep(1)
        cls.consumer = KafkaConsumer(*cls.topics,
                                     bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
                                     # auto_offset_reset='earliest',
                                     # enable_auto_commit=False,
                                     api_version=(0, 10, 1))
        cls.consume_messages()

    @classmethod
    def handle(cls, payload, handler):
        raise NotImplementedError

    @classmethod
    def consume_messages(cls):
        for message in cls.consumer:
            logging.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                        message.offset, message.key,
                                                        message.value))
            payload = json.loads(message.value.decode('utf-8'))
            for h in cls.get_appropriate_handlers(message.topic):
                cls.handle(payload, h)
        logging.info("Consuming finished.")

    @classmethod
    def stop(cls):
        cls.consumer.close(autocommit=False)

    @classmethod
    def update_topics(cls):
        cls.stop()
        cls.start()
