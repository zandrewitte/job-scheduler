from ppro.job_scheduler.consumers.consumers import Consumers
from ppro.job_scheduler.framework.logger import Logger

if __name__ == '__main__':
    logger = Logger.get_logger(__name__)
    logger.info("Starting Service")
    Consumers.consume()

