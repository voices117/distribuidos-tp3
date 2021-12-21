import os
import logging
import middleware

from pika import connection
from pipeline import *
from services import liveness_agent, storage


if __name__ == '__main__':
    RABBITMQ_ADDRESS = os.environ['RABBITMQ_ADDRESS']
    WORKER_TASK = os.environ['WORKER_TASK']
    WORKER_ID = os.environ['WORKER_ID']
    LOG_MESSAGES = os.environ.get('LOG_MESSAGES', '0')

    logging.getLogger("pika").setLevel(logging.WARNING)

    liveness_agent.start_server_in_new_thread()

    connection = middleware.connect(RABBITMQ_ADDRESS)
    channel = connection.channel()

    if LOG_MESSAGES != '0':
        print('****** LOG_MESSAGES ENABLED *******')
        middleware.LOG_MESSAGES = True

    try:
        middleware.setup_communication(channel=channel)
        storage.connect()

        print(WORKER_TASK, 'starting')
        middleware.execute_worker(name=WORKER_TASK, channel=channel)
    finally:
        channel.close()
        connection.close()

    exit(1)
