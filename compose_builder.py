import os
import math
import json
from service_config import WORKERS, LIBRARIANS, MAX_QUEUE_SIZE, TIMEOUT

COMPOSE_TEMPLATE = """
version: '3.4'

x-common-env-variables: &common-env-variables
    RABBITMQ_ADDRESS: rabbitmq
    PYTHONUNBUFFERED: 1

x-base-worker: &base-worker
    build:
        context: .
        dockerfile: ./Dockerfile
    command: "main.py"
    depends_on:
        - rabbitmq
    volumes:
        - ./:/app

x-base-storage-worker: &base-storage-worker
    build:
      context: .
      dockerfile: storage.Dockerfile
    command: python3.9 main.py
    volumes:
        - ./babel_library:/app
    networks:
      - storage_tp3_network

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
        environment:
            <<: *common-env-variables
            WORKER_TASK: {worker_type}
            WORKER_ID: {worker_id}
"""

STORAGE_SERVICE_TEMPLATE = """
    {name}:
        <<: *base-storage-worker
        environment:
            <<: *common-env-variables
            WORKER_ID: {worker_id}
            TIMEOUT: {timeout}
            MAX_QUEUE_SIZE: {max_queue_size}
            PORT: {port}
            QUORUM: {quorum}
            ARCHITECTURE: '{architecture}'
        ports:
            - "{port}:{port}"

"""

NETWORK_TEMPLATE = """
networks:
    storage_tp3_network:
        ipam:
            driver: default
            config:
                - subnet: 173.105.125.0/24

"""

def create_docker_compose():
    content = COMPOSE_TEMPLATE

    quorum = int(math.ceil(len(LIBRARIANS)/2.))
    architecture=json.dumps(LIBRARIANS)

    #####################
    ##################### PIPELINES WORKERS
    # for worker_type, replicas in WORKERS.items():
    #     if worker_type.startswith('client'):
    #         continue

    #     for i in range(replicas):
    #         content += SERVICE_TEMPLATE.format(worker_type=worker_type, worker_id=i)
    #####################
    #####################

    #####################
    ##################### STORAGE NODES
    for lib in LIBRARIANS:
        content += STORAGE_SERVICE_TEMPLATE.format(name=lib["name"],
        worker_id=lib["id"],
        timeout=TIMEOUT,
        max_queue_size=MAX_QUEUE_SIZE,
        port=lib["port"],
        quorum=quorum,
        architecture=architecture)
    #####################
    #####################

    content += NETWORK_TEMPLATE

    return content


if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 1, 'Usage  compose_builder.py'

    print(create_docker_compose())
