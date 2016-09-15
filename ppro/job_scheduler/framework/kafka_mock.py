import ast
import threading
from concurrent.futures import ThreadPoolExecutor
from ppro.job_scheduler.framework.logger import Logger
from ppro.job_scheduler.framework.singleton import Singleton

logger = Logger.get_logger(__name__)
messages = {}


class Producer(object):
    __metaclass__ = Singleton

    def __init__(self):
        self.fib_count = 1

    @staticmethod
    def publish(topic, obj, serializing_func, key=None, partition=None):
        if topic in messages.keys():
            messages[topic].append(str(serializing_func(obj)))
        else:
            messages[topic] = [str(serializing_func(obj))]

        logger.info('Message published to: %s, Payload: %s' % (topic, str(serializing_func(obj))))


class Consumer(object):
    __metaclass__ = Singleton

    def __init__(self):
        self.function_set = {}
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.consumeThread = threading.Thread(target=self.consume_async)
        self.subscriptions = []

    def add_subscription(self, topic_subscribe):
        for topic in topic_subscribe.topics:
            self.function_set.setdefault(topic, []).append(topic_subscribe)
            self.subscriptions.append(topic)

        if not self.consumeThread.isAlive():
            self.consumeThread.start()

    def consume_async(self):
        pass
        while True:
            for topic in self.subscriptions:
                try:
                    message = Message(topic, messages.get(topic, []).pop())
                    self.executor.submit(self.handle_message, self.function_set, message)
                except IndexError:
                    pass

    @staticmethod
    def handle_message(function_set, message):
        logger.debug('Received Message on Topic: %s, Payload: %s' % (message.topic, message.value))
        try:
            _ = function_set[message.topic]
            if message.value is not None:
                map(lambda v: v.handle_function(v.serializing_function(ast.literal_eval(message.value))), _)
        except Exception as e:
            logger.error('Error While Executing consumer function with Message (%s). Reason : %s'
                         % (message, e.message))


class TopicSubscribe(object):
    def __init__(self, topics=None, handle_function=None, serializing_function=None):
        self.topics = topics
        self.handle_function = handle_function
        self.serializing_function = serializing_function


def subscribe(topics=(), serializing_function=None):
    def decorated(f):
        c = Consumer()
        c.add_subscription(TopicSubscribe(topics, f, serializing_function))

        logger.info('Consumer Subscribed to: %s' % topics)
    return decorated


class Message(object):
    def __init__(self, topic, value):
        self.topic = topic
        self.value = value
