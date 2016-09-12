from ppro.job_scheduler.event import Event
from ppro.job_scheduler.framework.kafka_queue import subscribe
from ppro.job_scheduler.topics import Topics
from ppro.job_scheduler.framework.logger import Logger

logger = Logger.get_logger(__name__)


class JobConsumer(object):

    def __init__(self):
        pass

    @staticmethod
    @subscribe(Topics.PlayerPro.Incoming.Event, Event.deserialize)
    def consume_event(event):
        logger.info('requestUUID : %s \n' % event.header.get("requestUUID"))
        logger.info('userUUID : %s \n' % event.meta.get("userUUID"))
        for element in event.payload.get('body'):
            logger.info(element.get('id'))
