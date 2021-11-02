import os
import pika
import time
import service_config

from typing import Generator
from functools import wraps
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


# special message payload that indicates the worker is done producing output.
# It is used by each stage of the pipeline to indicate the next stage that
# it shouldn't expect more data and propagate the signal. Each worker will
# send a "DONE" message for each worker in the next stage. Workers should
# continue consuming until they receive a number of DONE packages equal to the
# number of workers in the previous stage (see WORKERS_TO_WAIT).
DONE = b''


# creates a map containing the number of workers in the previous stage of
# each stage in the pipeline. With this information, each worker knows how
# many "DONE" messages it needs to expect
WORKERS_TO_WAIT = defaultdict(int)
for task, next_tasks in service_config.NEXT_TASK.items():
    for next in next_tasks:
        WORKERS_TO_WAIT[next] += 1 * service_config.WORKERS[task]


# map of workers registered by `as_worker`
REGISTERED_WORKERS = {}


def connect(address:str, retries:int = 15) -> BlockingConnection:
    """Connects to a RabbitMQ instance in the given address with the defined
    number of retries."""

    success = False
    for _ in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(address))
            success = True
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    
    if not success:
        raise Exception('could not connect to rabbit MQ')

    return connection


def setup_communication(channel:BlockingChannel) -> None:
    """Creates the required queues and exchanges for the pipeline to work."""

    for worker in service_config.WORKERS:
        if worker.startswith('client'):
            continue

        if worker in service_config.SHARDED:
            channel.exchange_declare(exchange=worker, exchange_type='direct')
        else:
            channel.queue_declare(queue=worker, durable=False)


def execute_worker(name:str, channel:BlockingChannel) -> None:
    """Executes the corresponding worker by its name. The worker must be
    registered using the `as_worker` decorator."""

    REGISTERED_WORKERS[name](channel)


def send_data(data:bytes, channel:BlockingChannel, worker:str, id='', shard_key=None):
    """Sends `data` to a stage of the pipeline. `worker` is the name of the
    workers in the destination stage. The optional `id` parameter is passed as
    the message correlation ID.
    The `shard_key` is for sharded stages, where each worker expects to receive
    a subset of the messages based on the `shard_key`. This is only used for
    workers that are in the SHARDED list in the `service_config`."""

    exchange, routing_key = '', worker

    if worker in service_config.SHARDED:
        # if sharded then it uses an exchange routed by the shard key
        exchange, routing_key = worker, shard_key

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=data,
        mandatory=1,
        properties=pika.BasicProperties(
            delivery_mode=1,  # transient delivery mode
            correlation_id=id,
        )
    )


def send_done(channel:BlockingChannel, worker:str):
    """Sends a special package indicating the worker has finished sending all
    the data."""

    num_messages = service_config.WORKERS[worker]
    for i in range(num_messages):
        send_data(data=DONE, channel=channel, worker=worker, shard_key=str(i))


def as_worker(task_callback):
    """Decorator that wraps a function and handles group communication. Once
    the wrapped function exits, this decorator will send a "DONE" message to
    each worker on the next stage.
    To find out which workers are on the next stage is uses the adjacency list
    of the processing DAG found in service_config.
    Note that we expect several nodes running the same code on different
    containers but processing the data in parallel from the same queues."""
    
    worker_name = task_callback.__name__.replace('_callback', '')
    
    @wraps(task_callback)
    def _wrapper(channel:BlockingChannel):
        task_callback(channel)

        # once the worker finishes, we send the DONE signal to the following
        # stages
        for next_task in service_config.NEXT_TASK[worker_name]:
            send_done(channel=channel, worker=next_task)

        print(worker_name, 'finished')

    REGISTERED_WORKERS[worker_name] = _wrapper
    return _wrapper


def consume_from(channel:BlockingChannel, worker_name:str) -> Generator:
    """Yields messages received from the indicated stage `worker_name` until
    all "DONE" signals are received."""

    if worker_name in service_config.SHARDED:
        # sharded stages guarantee that the messages are routed by the worker
        # unique ID. Thus, we use an exclusive queue and a "direct" exchange
        result = channel.queue_declare(queue='', exclusive=True, durable=False)
        queue_name = result.method.queue

        channel.queue_bind(
            exchange=worker_name,
            queue=queue_name,
            routing_key=os.environ['WORKER_ID']  # each worker is assigned a
        )                                        # unique ID on their group
    else:
        # normal stages use a named queue as the routing key
        queue_name = worker_name

    # starts consuming events from the queue
    done_messages_received = 0
    for method_frame, properties, body in channel.consume(queue_name, auto_ack=False):
        if body == DONE:
            done_messages_received += 1

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            if _is_task_done(worker_name, done_messages_received):
                print(worker_name, 'exiting, received', done_messages_received, 'done')
                return

            continue

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        yield method_frame, properties, body


def _is_task_done(worker_name:str, done_messages_received:int):
    """Checks if the given worker has already received all the "DONE" messages
    from the previous' stage workers."""

    print(worker_name, 'received', done_messages_received, 'but expects', WORKERS_TO_WAIT[worker_name])
    return done_messages_received == WORKERS_TO_WAIT[worker_name]
