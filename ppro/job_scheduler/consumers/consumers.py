import job_consumer


class Consumers:
    def __init__(self):
        pass

    @staticmethod
    def consume():
        job_consumer.JobConsumer()
