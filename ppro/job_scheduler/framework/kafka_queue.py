import ast
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import ppro.job_scheduler.framework.yaml_reader
from ppro.job_scheduler.framework.singleton import Singleton
from ppro.job_scheduler.framework.logger import Logger

logger = Logger.get_logger(__name__)
default_config_location = './conf/kafka.yaml'


class Producer(object):
    __metaclass__ = Singleton

    def __init__(self, config_location=default_config_location):
        read = ppro.job_scheduler.framework.yaml_reader.read_yaml(config_location)
        self.__connect__(read)

    def __connect__(self, read):
        try:
            logger.info('Connecting to Kafka Cluster')
            self.producer = KafkaProducer(**dict(read.get('kafka', {}).items() + read.get('kafka-producer', {}).items()))
        except NoBrokersAvailable as conn_err:
            logger.info('No Brokers Available on Cluster, Retrying in 5 seconds')
            time.sleep(5)
            self.__connect__(read)

    def publish(self, topic, obj, serializing_func, key=None, partition=None):
        self.producer.send(topic, str(serializing_func(obj)), key, partition)
        logger.info('Message published to: %s, Payload: %s' % (topic, str(serializing_func(obj))))


class Consumer(object):
    __metaclass__ = Singleton

    def __init__(self, config_location=default_config_location):
        read = ppro.job_scheduler.framework.yaml_reader.read_yaml(config_location)
        self.__connect__(read)
        self.function_set = {}
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.consumeThread = threading.Thread(target=self.consume_async)

    def __connect__(self, read):
        try:
            logger.info('Connecting to Kafka Cluster')
            self.consumer = KafkaConsumer(**dict(read.get('kafka', {}).items() + read.get('kafka-consumer', {}).items() +
                                                 [('value_deserializer', self.message_serializer)]))
        except NoBrokersAvailable as conn_err:
            logger.info('No Brokers Available on Cluster, Retrying in 5 seconds')
            time.sleep(5)
            self.__connect__(read)

    def add_subscription(self, topic_subscribe):
        if self.consumer.subscription() is not None:
            self.consumer.subscribe(list(self.consumer.subscription() | {topic_subscribe.topic}))
        else:
            self.consumer.subscribe([topic_subscribe.topic])
        self.function_set[topic_subscribe.topic] = topic_subscribe
        if not self.consumeThread.isAlive():
            self.consumeThread.start()

    def subscribe(self, *topic_subscribes):
        topics = list(map(lambda t_sub: t_sub.topic, topic_subscribes))
        self.consumer.subscribe(topics)
        self.function_set = dict(zip(topics, topic_subscribes))
        if not self.consumeThread.isAlive():
            self.consumeThread.start()

    @staticmethod
    def message_serializer(message):
        try:
            return ast.literal_eval(message)
        except Exception as e:
            logger.error('Error While Serializing Message (%s). Reason : %s' % (message, e.message))

    def consume_async(self):
        for message in self.consumer:
            self.executor.submit(self.handle_message, self.function_set, message)

    @staticmethod
    def handle_message(function_set, message):
        logger.debug('Received Message on Topic: %s, Payload: %s' % (message.topic, message.value))
        try:
            _ = function_set[message.topic]
            if message.value is not None:
                _.handle_function(_.serializing_function(message.value))
        except Exception as e:
            logger.error('Error While Executing consumer function with Message (%s). Reason : %s'
                         % (message, e.message))


class TopicSubscribe(object):
    def __init__(self, topic, handle_function, serializing_function):
        self.topic = topic
        self.handle_function = handle_function
        self.serializing_function = serializing_function


def subscribe(topic, serializing_function, config_location=None):
    def decorated(f):
        if config_location is not None:
            c = Consumer(config_location)
        else:
            c = Consumer()
        c.add_subscription(TopicSubscribe(topic, f, serializing_function))

        logger.info('Consumer Subscribed to: %s' % topic)
    return decorated
