from ppro.job_scheduler.models.event import Event, Payload, Request
from datetime import datetime
from ppro.job_scheduler.framework.kafka_queue import Producer
from ppro.job_scheduler.models.topics import Topics
import time


class EventProducer(object):
    def __init__(self):
        pass

    @staticmethod
    def produce():
        while True:
            event = Event('Player', 'Create', 'http://service/enpoint/resourceid', 'e_tag_value',
                          datetime.now().isoformat(), 'correlation_id_value', 'http://service/endpoint/events',
                          Payload({
                              'field_e_1': 'value_e_1',
                              'field_e_2': 'value_e_2',
                              'field_e_3': 'value_e_3'
                          }, {
                              'field_d_1': 'value_d_1',
                              'field_d_2': 'value_d_2',
                              'field_d_3': 'value_d_3'
                          }),
                          Request('POST', 'some_path', {
                              'etag': 'some_etag',
                              'content-type': 'application/json',
                              'content-length': '123',
                              'user-agent': 'some_user_agent',
                              'host': '127.0.0.1'
                          }, 'someparam=somevalue',
                                  {
                                      'remoteAddress': 'some_remote_address'
                                  }
                                  ))

            Producer().publish(Topics.PlayerPro.Incoming.Event, event, Event.serialize)
            time.sleep(.5)
