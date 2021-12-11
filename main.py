import os
import middleware

from pika import connection
from pipeline import *
from services import liveness_agent


if __name__ == '__main__':
    RABBITMQ_ADDRESS = os.environ['RABBITMQ_ADDRESS']
    WORKER_TASK = os.environ['WORKER_TASK']
    WORKER_ID = os.environ['WORKER_ID']

    liveness_agent.start_server_in_new_thread()

    connection = middleware.connect(RABBITMQ_ADDRESS)
    channel = connection.channel()

    try:
        middleware.setup_communication(channel=channel)

        print(WORKER_TASK, 'starting')
        middleware.execute_worker(name=WORKER_TASK, channel=channel)
    finally:
        channel.close()
        connection.close()
