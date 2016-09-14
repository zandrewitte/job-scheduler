import ast
import os
import threading
import time
import re
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import ppro.job_scheduler.framework.yaml_reader
from ppro.job_scheduler.framework.logger import Logger
from ppro.job_scheduler.framework.singleton import Singleton

logger = Logger.get_logger(__name__)
default_config_location = '/etc/kafka.yaml'


class Producer(object):
    __metaclass__ = Singleton

    def __init__(self, config_location=default_config_location):
        read = ppro.job_scheduler.framework.yaml_reader.read_yaml(os.getcwd() + config_location)
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
        read = ppro.job_scheduler.framework.yaml_reader.read_yaml(os.getcwd() + config_location)
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
            self.consumer.subscribe(list(self.consumer.subscription() | set(topic_subscribe.topics)))
        else:
            self.consumer.subscribe(topic_subscribe.topics)

        for topic in topic_subscribe.topics:
            self.function_set.setdefault(topic, []).append(topic_subscribe)

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
                map(lambda v: v.handle_function(v.serializing_function(message.value)), _)
        except Exception as e:
            logger.error('Error While Executing consumer function with Message (%s). Reason : %s'
                         % (message, e.message))


class TopicSubscribe(object):
    def __init__(self, topics=None, handle_function=None, serializing_function=None):
        self.topics = topics
        self.handle_function = handle_function
        self.serializing_function = serializing_function


def subscribe(topics=(), serializing_function=None, config_location=None):
    def decorated(f):
        if config_location is not None:
            c = Consumer(config_location)
        else:
            c = Consumer()
        c.add_subscription(TopicSubscribe(topics, f, serializing_function))

        logger.info('Consumer Subscribed to: %s' % topics)
    return decorated


class WildCardConsumer(object):
    __metaclass__ = Singleton

    def __init__(self, config_location=default_config_location):
        read = ppro.job_scheduler.framework.yaml_reader.read_yaml(os.getcwd() + config_location)
        self.__connect__(read)
        self.function_set = {}
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.consumeThread = threading.Thread(target=self.consume_async, name='consume_async_wildcard')

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
            self.consumer.subscribe(pattern=topic_subscribe.topics)
        else:
            self.consumer.subscribe(pattern=topic_subscribe.topics)

        self.function_set[topic_subscribe.topics] = topic_subscribe

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
        logger.info('Received Message on Topic: %s, Payload: %s' % (message.topic, message.value))
        try:
            logger.info(function_set[message.topic])
            logger.info(function_set)

            matches = {key: value for key, value in function_set.items() if re.search(key, message.topic) is not None}

            logger.info(matches)
            # if message.value is not None:
            #     map(lambda v: v.handle_function(v.serializing_function(message.value)), _)
        except Exception as e:
            logger.error('Error While Executing consumer function with Message (%s). Reason : %s'
                         % (message, e.message))


def wildcard_subscribe(pattern='', serializing_function=None, config_location=None):
    def decorated(f):
        if config_location is not None:
            c = WildCardConsumer(config_location)
        else:
            c = WildCardConsumer()
        c.add_subscription(TopicSubscribe(pattern, f, serializing_function))

        logger.info('Consumer Subscribed to: %s' % pattern)
    return decorated
