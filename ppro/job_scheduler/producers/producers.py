import event_producer


class Producers:
    def __init__(self):
        pass

    @staticmethod
    def produce():
        event_producer.EventProducer.produce()
