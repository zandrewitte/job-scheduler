from marshmallow import Schema, fields, post_load
import ast


class Event(object):
    def __init__(self, header, meta, payload):
        self.header = header
        self.meta = meta
        self.payload = payload

    def serialize(self):
        return EventSchema(strict=True).dump(self).data

    @staticmethod
    def deserialize(json):
        return EventSchema(strict=True).load(json).data


class EventSchema(Schema):
    header = fields.Str()
    meta = fields.Str()
    payload = fields.Str()

    @post_load
    def make_event(self, data):
        return Event(ast.literal_eval(data["header"]), ast.literal_eval(data["meta"]),
                     ast.literal_eval(data["payload"]))

