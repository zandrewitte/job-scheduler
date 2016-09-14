from ppro.job_scheduler.framework.kafka_queue import subscribe
from ppro.job_scheduler.framework.logger import Logger
from ppro.job_scheduler.models.event import Event

from ppro.job_scheduler.models.topics import Topics

logger = Logger.get_logger(__name__)


class JobConsumer(object):

    @staticmethod
    @subscribe(Topics.PlayerPro.Incoming.Event, Event.deserialize)
    def consume_event(event):
        logger.info(event)
