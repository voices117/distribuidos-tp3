import os

from service_config import WORKERS


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


def create_docker_compose():
    content = COMPOSE_TEMPLATE

    for worker_type, replicas in WORKERS.items():
        if worker_type.startswith('client'):
            continue

        for i in range(replicas):
            content += SERVICE_TEMPLATE.format(worker_type=worker_type, worker_id=i)

    return content


if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 1, 'Usage  compose_builder.py'

    print(create_docker_compose())
