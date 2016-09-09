import logging
from ppro.job_scheduler.job_consumer import JobConsumer


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel('INFO')
    formatter = logging.Formatter(
        '%(asctime)s {} %(name)s in PLAYERPRO-JOB-SCHEDULER: %(levelname)s %(message)s, '
        'line: %(lineno)d in %(funcName)s, %(filename)s Created: %(created)f'
        .format('localhost'), datefmt='%b %d %H:%M:%S')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # file_handler = logging.FileHandler('/tmp/job_scheduler.log')
    # logger.addHandler(file_handler)

    JobConsumer()
