from marshmallow import Schema, fields, post_load


class Event(object):
    def __init__(self, resource_type, action_type, resource_url, e_tag, timestamp, correlation_id, source, payloads,
                 request=None):
        self.resource_type = resource_type
        self.action_type = action_type
        self.resource_url = resource_url
        self.e_tag = e_tag
        self.timestamp = timestamp
        self.correlation_id = correlation_id
        self.source = source
        self.payloads = payloads
        self.request = request

    def serialize(self):
        return EventSchema(strict=True).dump(self).data

    @staticmethod
    def deserialize(json):
        return EventSchema(strict=True).load(json).data


class Request(object):
    def __init__(self, method, path, headers, params, meta):
        self.method = method
        self.path = path
        self.headers = headers
        self.params = params
        self.meta = meta


class Payload(object):
    def __init__(self, event, diagnostic):
        self.event = event
        self.diagnostic = diagnostic


class RequestSchema(Schema):
    method = fields.Str()
    path = fields.Str()
    headers = fields.Dict()
    params = fields.Str()
    meta = fields.Dict()

    @post_load
    def make_request(self, data):
        return Request(data["method"], data["path"], data["headers"], data["params"], data["meta"])


class PayloadSchema(Schema):
    event = fields.Dict()
    diagnostic = fields.Dict()

    @post_load
    def make_payload(self, data):
        return Payload(data["event"], data["diagnostic"])


class EventSchema(Schema):
    resource_type = fields.Str()
    action_type = fields.Str()
    resource_url = fields.Str()
    e_tag = fields.Str()
    timestamp = fields.Str()
    correlation_id = fields.Str()
    source = fields.Str()
    request = fields.Nested(RequestSchema, required=False, allow_none=True, default=None)
    payloads = fields.Nested(PayloadSchema)

    @post_load
    def make_event(self, data):
        return Event(data["resource_type"], data["action_type"], data["resource_url"], data["e_tag"], data["timestamp"],
                     data["correlation_id"], data["source"], data["payloads"], data["request"])

