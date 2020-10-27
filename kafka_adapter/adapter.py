from kafka import KafkaConsumer
from datalake.settings import KAFKA_HOST, KAFKA_PORT
from kafka_adapter.handler import KafkaBaseHandler
import logging


class KafkaAdapter:
    topics = ['_always_empty_topic']
    handlers = []
    consumer = None

    @classmethod
    def get_available_kafka_handlers(cls):
        return KafkaBaseHandler.__subclasses__()

    @classmethod
    def get_appropriate_handlers(cls, kafka_topic, available_handlers):
        res = []
        for h in available_handlers:
            if h.get_topic() == kafka_topic:
                res.append(h)
        return res

    @classmethod
    def start(cls):
        cls.handlers = cls.get_available_kafka_handlers()
        logging.info(f"Available handlers: {cls.handlers}")
        cls.consumer = KafkaConsumer(*cls.topics,
                                     bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
                                     # auto_offset_reset='earliest',
                                     # enable_auto_commit=False,
                                     api_version=(0, 10, 1))
        cls.update_topics()
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
            payload = message.value.decode('utf-8')
            for h in cls.get_appropriate_handlers(message.topic, cls.handlers):
                cls.handle(payload, h)
        logging.info("Consuming finished.")

    @classmethod
    def stop(cls):
        cls.consumer.close(autocommit=False)

    @classmethod
    def update_topics(cls):
        topics = [h.get_topic() for h in cls.handlers]
        cls.topics = topics if topics else ['_always_empty_topic']
        cls.consumer.subscribe(cls.topics)
