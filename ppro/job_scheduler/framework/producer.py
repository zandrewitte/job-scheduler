import uuid

from ppro.job_scheduler.framework.kafka_queue import Producer
from ppro.job_scheduler.models.event import Event

from ppro.job_scheduler.models.topics import Topics

while True:
    for i in range(1):

        eventHeader = {
            'requestUUID': str(uuid.uuid4()),
            'eventType': 'SavedEvent',
            'someField': 'SomeValue'
        }

        eventMeta = {
            'userUUID': str(uuid.uuid4()),
            'someMeta': 'SomeMetaValue'
        }

        eventPayload1 = {
            "body": [
                {
                    "description": "This is a description edit",
                    "privacy": "public",
                    "eventType": "Practice",
                    "rsvps": {
                        "missing": 0,
                        "maybe": 0,
                        "invited": 0,
                        "going": 0,
                        "declined": 0
                    },
                    "venue": {
                        "id": 200,
                        "city": "Cape Elizabeth",
                        "fields": [
                            {
                                "name": "Cape Elizabeth High School",
                                "id": 200
                            }
                        ],
                        "name": "Cape Elizabeth High School",
                        "address": "345 Ocean House Road, Cape Elizabeth, ME 04107, United States"
                    },
                    "field": {
                        "name": "Cape Elizabeth High School",
                        "id": 200
                    },
                    "isMember": False,
                    "team": {
                        "username": "Teamofultimatepower",
                        "name": "Team of ultimate power",
                        "crestPicture": {
                            "url": "https://playerpro-upload.s3.amazonaws.com/2015/11/02/563740c089c73.gif",
                            "width": 200,
                            "thumbnailUrl": "https://playerpro-upload.s3.amazonaws.com/2015/11/02/563740c089c73.gif",
                            "id": 2110,
                            "height": 200
                        },
                        "coverPhoto": {
                            "url": "https://s3-us-west-1.amazonaws.com/playerpro-assets/soccer-training2.jpg",
                            "thumbnailUrl": "https://s3-us-west-1.amazonaws.com/playerpro-assets/soccer-training2.jpg",
                        },
                        "id": 1370,
                    },
                    "date": "2015-11-07T01:53:00Z",
                    "isManager": False,
                    "id": 293
                }
            ],
            "message": "Retrieved event OK",
        }
        eventPayload2 = {
            "body": [
                {
                    "description": "test",
                    "privacy": "public",
                    "eventType": "Pickup",
                    "rsvps": {
                        "missing": 0,
                        "maybe": 0,
                        "invited": 0,
                        "going": 0,
                        "declined": 0
                    },
                    "venue": {
                        "id": 473,
                        "city": "",
                        "fields": [
                            {
                                "name": "Tokyo, Japan",
                                "id": 487
                            }
                        ],
                        "name": "Tokyo, Japan",
                        "address": "Tokyo, Japan"
                    },
                    "field": {
                        "name": "Tokyo, Japan",
                        "id": 487
                    },
                    "title": "Test",
                    "date": "2015-10-22T17:38:00Z",
                    "isManager": False,
                    "organizer": {
                    },
                    "id": 235
                }
            ],
            "message": "Retrieved event OK",
        }
        eventPayload3 = {
            "body": [
                {
                    "permaLink": "http://beta.getplayerpro.com/calendar/meeting/1",
                    "isManager": False,
                    "description": "",
                    "privacy": "public",
                    "eventType": "Meeting",
                    "rsvps": {
                        "meStatus": "missing",
                        "missing": 0,
                        "maybe": 1,
                        "invited": 0,
                        "going": 0,
                        "declined": 0
                    },
                    "venue": {
                        "id": 472,
                        "city": "San Francisco",
                        "fields": [
                            {
                                "name": "1590 Bryant Street, San Francisco, CA 94103, United States",
                                "id": 486
                            }
                        ],
                        "name": "1590 Bryant Street, San Francisco, CA 94103, United States",
                        "address": "1590 Bryant Street, San Francisco, CA 94103, United States"
                    },
                    "title": "GGWSL -  General Meeting",
                    "isMember": False,
                    "date": "2015-04-14T07:00:00Z",
                    "organizer": {
                        "name": "Golden Gate Womens Soccer League",
                        "crestPicture": {
                            "url": "https://playerpro-upload.s3.amazonaws.com/2015/05/24/55624bd7627bf.png",
                            "width": 695,
                            "thumbnailUrl": "https://playerpro-upload.s3.amazonaws.com/2015/05/24/55624bd7627bf_thumbnail.png",
                            "id": 2084,
                            "height": 686
                        },
                        "coverPhoto": {
                            "url": "https://playerpro-upload.s3.amazonaws.com/2015/06/02/556cfe04d4818.jpg",
                            "width": 1920,
                            "thumbnailUrl": "https://playerpro-upload.s3.amazonaws.com/2015/06/02/556cfe04d4818_thumbnail.png",
                            "id": 2267,
                            "height": 523
                        },
                        "crestPictureUrl": "https://playerpro-upload.s3.amazonaws.com/2015/05/24/55624bd7627bf_thumbnail.png",
                        "type": "Competition",
                        "id": 1
                    },
                    "id": 1
                }
            ],
            "message": "Retrieved event OK",
        }

        Producer().publish(Topics.PlayerPro.Incoming.Event, Event(eventHeader, eventMeta, eventPayload1),
                           Event.serialize)
        Producer().publish(Topics.PlayerPro.Incoming.Event, Event(eventHeader, eventMeta, eventPayload2),
                           Event.serialize)
        Producer().publish(Topics.PlayerPro.Incoming.Event, Event(eventHeader, eventMeta, eventPayload3),
                           Event.serialize)
        # Producer().publish('topic3', User('some_other-%s@mail.com' % i, 'test'), User.serialize)
