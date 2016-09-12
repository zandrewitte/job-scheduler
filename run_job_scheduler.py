from ppro.job_scheduler.framework.logger import Logger
from ppro.job_scheduler.job_consumer import JobConsumer


if __name__ == '__main__':
    logger = Logger.get_logger(__name__)
    logger.info("Starting Service")

    JobConsumer()
