import os
import math
import json
from service_config import WORKERS, LIBRARIANS, MAX_QUEUE_SIZE, TIMEOUT, NUMBER_OF_MONITOR_CONTAINERS

COMPOSE_TEMPLATE = """
version: '3.4'

x-common-env-variables: &common-env-variables
    RABBITMQ_ADDRESS: rabbitmq
    PYTHONUNBUFFERED: 1
    LOG_MESSAGES: 1

x-base-worker: &base-worker
    build:
        context: .
        dockerfile: ./Dockerfile
    command: "main.py"
    depends_on:
        - rabbitmq
    volumes:
        - ./:/app
        - ./logs:/logs

x-base-storage-worker: &base-storage-worker
    build:
      context: .
      dockerfile: Dockerfile.storage
    command: python3.9 /app/storage_main.py
    volumes:
        - ./:/app

services:
    rabbitmq:
        container_name: rabbitmq
        image: rabbitmq:3.9-management
        ports:
            - 5672:5672
            - 15672:15672

"""

SERVICE_TEMPLATE = """
    {worker_type}_{worker_id}:
        <<: *base-worker
        container_name: {worker_type}_{worker_id}
        environment:
            <<: *common-env-variables
            WORKER_TASK: {worker_type}
            WORKER_ID: {worker_id}
"""

STORAGE_SERVICE_TEMPLATE = """
    {name}:
        <<: *base-storage-worker
        container_name: {name}
        environment:
            <<: *common-env-variables
            WORKER_ID: {worker_id}
            WORKER_TASK: 'storage'
            TIMEOUT: {timeout}

"""

MONITOR_SERVICE_TEMPLATE = """
    monitor_{number}:
        container_name: monitor_{number}
        build:
          context: .
          dockerfile: Dockerfile.monitor
        environment:
            WORKER_TASK: monitor
            WORKER_ID: {number}
        depends_on:
            - rabbitmq
        volumes:
          - /var/run/docker.sock:/var/run/docker.sock
          - ./monitor/src/config{number}.json:/config.json
          - ./killer_conf:/app/killer_conf
"""

def create_docker_compose():
    content = COMPOSE_TEMPLATE

    quorum = int(math.ceil(len(LIBRARIANS)/2.))
    architecture=json.dumps(LIBRARIANS)

    #####################
    ##################### PIPELINES WORKERS
    #for worker_type, replicas in WORKERS.items():
    #    if worker_type.startswith('client'):
    #        continue
#
    #    for i in range(replicas):
    #        content += SERVICE_TEMPLATE.format(worker_type=worker_type, worker_id=i)

    #####################
    ##################### STORAGE NODES
    for lib in LIBRARIANS:
       content += STORAGE_SERVICE_TEMPLATE.format(name=lib["name"],
       worker_id=lib["id"],
       timeout=TIMEOUT)
    #####################
    #####################

    for monitmonitor_number in range(1, NUMBER_OF_MONITOR_CONTAINERS+1):
        content += MONITOR_SERVICE_TEMPLATE.format(number = monitmonitor_number)


    return content


if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 1, 'Usage  compose_builder.py'

    print(create_docker_compose())
