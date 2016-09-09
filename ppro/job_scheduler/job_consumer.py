from ppro.job_scheduler.event import Event
from ppro.job_scheduler.framework.kafka_queue import subscribe
from ppro.job_scheduler.topics import Topics


class JobConsumer(object):

    @staticmethod
    @subscribe(Topics.PlayerPro.Incoming.Event, Event.deserialize)
    def consume_event(event):
        print 'requestUUID : %s \n' % event.header.get("requestUUID")
        print 'userUUID : %s \n' % event.meta.get("userUUID")
        for element in event.payload.get('body'):
            print element.get('id')
