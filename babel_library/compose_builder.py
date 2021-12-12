import os
import math
import json
from config import LIBRARIANS, MAX_QUEUE_SIZE, TIMEOUT

COMPOSE_TEMPLATE = """
version: '3.4'

x-common-env-variables: &common-env-variables
    PYTHONUNBUFFERED: 1
    

x-base-worker: &base-worker
    build:
      context: .
      dockerfile: Dockerfile
    command: python3.9 main.py
    volumes:
        - ./:/app
    networks:
      - storage_tp3_network

services:
"""

SERVICE_TEMPLATE = """
    {name}:
        <<: *base-worker
        environment:
            <<: *common-env-variables
            WORKER_ID: {worker_id}
            TIMEOUT: {timeout}
            MAX_QUEUE_SIZE: {max_queue_size}
            PORT: {port}
            QUORUM: {quorum}
            ARCHITECTURE: '{architecture}'

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

    for lib in LIBRARIANS:
        content += SERVICE_TEMPLATE.format(name=lib["name"],
        worker_id=lib["id"],
        timeout=TIMEOUT,
        max_queue_size=MAX_QUEUE_SIZE,
        port=lib["port"],
        quorum=quorum,
        architecture=architecture)

    content += NETWORK_TEMPLATE

    return content


if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 1, 'Usage  compose_builder.py'

    print(create_docker_compose())
