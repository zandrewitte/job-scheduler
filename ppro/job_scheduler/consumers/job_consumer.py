from ppro.job_scheduler.framework.kafka_queue import subscribe, wildcard_subscribe
from ppro.job_scheduler.framework.logger import Logger
from ppro.job_scheduler.models.event import Event

from ppro.job_scheduler.models.topics import Topics

logger = Logger.get_logger(__name__)


class JobConsumer(object):

    # @staticmethod
    # @subscribe([Topics.PlayerPro.Incoming.Event], Event.deserialize)
    # def consume_event(event):
    #     logger.info(event.payloads)
    #     logger.info(event.request)
    #
    # @staticmethod
    # @subscribe([Topics.PlayerPro.Incoming.Event, Topics.PlayerPro.Jobs.Analytics], Event.deserialize)
    # def consume_event1(event):
    #     logger.info("1")

    @staticmethod
    @wildcard_subscribe('playerpro-incoming-*', Event.deserialize)
    def consume_event2(event):
        logger.info("2")

    # @staticmethod
    # @wildcard_subscribe('playerpro-job-*', Event.deserialize)
    # def consume_event2(event):
    #     logger.info("3")
