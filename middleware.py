import re
import os
import json
import pika
import time
import hashlib
import logging

import service_config

from typing import Callable, Dict, Generator, List, Optional, Set, Tuple, Union
from services import storage, killer
from functools import wraps
from collections import defaultdict
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


# if True, all messages send to workers are written to files in /logs
LOG_MESSAGES = False
LOG_FILES = {}


# special message payload that indicates the worker is done producing output.
# It is used by each stage of the pipeline to indicate the next stage that
# it shouldn't expect more data and propagate the signal. Each worker will
# send a "DONE" message for each worker in the next stage. Workers should
# continue consuming until they receive a number of DONE packages equal to the
# number of workers in the previous stage (see WORKERS_TO_WAIT).
# the DONE message consist of an initial byte \x00, then the ID of the worker
# that sent the message and finally the ID of the worker meant to receive
# the message
DONE_RE = re.compile('^\x00(.+?)\x1c(\\d+)$')


# creates a map containing the number of workers in the previous stage of
# each stage in the pipeline. With this information, each worker knows how
# many "DONE" messages it needs to expect
WORKERS_TO_WAIT:Dict[str, int] = defaultdict(int)
for task, next_tasks in service_config.NEXT_TASK.items():
    for next_task in next_tasks:
        WORKERS_TO_WAIT[next_task] += 1 * service_config.WORKERS[task]


# map of workers registered by `as_worker`
REGISTERED_WORKERS:Dict[str, Callable] = {}

# unique identifier for this worker node
WORKER_ID = os.environ['WORKER_ID']
WORKER_TASK = os.environ['WORKER_TASK']
STORAGE_ID = f"{WORKER_TASK}_{WORKER_ID}"

# name of the exchange where the client output is sent
CLIENT_RESPONSE_EXCHANGE = 'client_output'


class END_OF_STREAM:
    """Just a type to indicate that a stream finished."""
    pass


class USE_HASH:
    """Just a type to indicate that the shard key of a message should be the hash."""
    pass


def connect(address:str, retries:int = 25) -> BlockingConnection:
    """Connects to a RabbitMQ instance in the given address with the defined
    number of retries."""

    try:
        # temporally disable logging to avoid getting spam messages
        logging.getLogger("pika").propagate = False

        for _ in range(retries):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(address))
                return connection
            except pika.exceptions.AMQPConnectionError:
                time.sleep(1)
        
        # at this point we failed to get a connection
        raise Exception('could not connect to rabbit MQ')
    finally:
        logging.getLogger("pika").propagate = True


def setup_communication(channel:BlockingChannel) -> None:
    """Creates the required queues and exchanges for the pipeline to work."""

    # creates the queue where the clients receive the output
    channel.exchange_declare(exchange=CLIENT_RESPONSE_EXCHANGE, exchange_type='direct')

    for worker in service_config.WORKERS:
        if worker.startswith('client'):
            continue

        if worker in service_config.SHARDED:
            channel.exchange_declare(exchange=worker, exchange_type='direct')
        else:
            channel.queue_declare(queue=worker, durable=True)


def execute_worker(name:str, channel:BlockingChannel) -> None:
    """Executes the corresponding worker by its name. The worker must be
    registered using the `as_worker` decorator."""

    REGISTERED_WORKERS[name](channel, worker_id=WORKER_ID)


def send_data(data:Union[bytes, str], channel:BlockingChannel, worker:str, correlation_id:str, shard_key:Optional[Union[int, USE_HASH]]=None):
    """Sends `data` to a stage of the pipeline. `worker` is the name of the
    workers in the destination stage. The `correlation_id` parameter is passed as
    the message correlation ID and it's used to identify the request that the chunk is
    associated with.
    The `shard_key` is for sharded stages, where each worker expects to receive
    a subset of the messages based on the `shard_key`. This is only used for
    workers that are in the SHARDED list in the `service_config`."""

    if isinstance(data, str):
        data = data.encode('utf-8')

    if isinstance(shard_key, USE_HASH) or shard_key is USE_HASH:
        shard_key = hash(data)

    if shard_key is not None:
        assert isinstance(shard_key, int), repr(shard_key)
        shard_key = shard_key % service_config.WORKERS[worker]

    exchange, routing_key = '', worker

    if worker in service_config.SHARDED:
        assert shard_key is not None, 'shard_key is mandatory for sharded workers'
    
        # if sharded then it uses an exchange routed by the shard key
        exchange, routing_key = worker, str(shard_key)

    if LOG_MESSAGES:
        file_name = f'/logs/{correlation_id}/output/{exchange}-{routing_key}.txt'
        if file_name not in LOG_FILES:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            LOG_FILES[file_name] = open(f'/logs/{correlation_id}/output/{exchange}-{routing_key}.txt', 'wb', buffering=0)

        LOG_FILES[file_name].write(data + b'\n')

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=data,
        mandatory=1,
        properties=pika.BasicProperties(
            #delivery_mode=1,  # transient delivery mode
            correlation_id=correlation_id,
        )
    )


def send_to_client(channel:BlockingChannel, correlation_id:str, body:bytes):
    """Sends a message to a client identified by the `correlation_id`. This function is
    used to send the final pipeline response to the clients."""

    channel.basic_publish(
        exchange=CLIENT_RESPONSE_EXCHANGE,
        routing_key=correlation_id,
        body=body,
        mandatory=1,
        properties=pika.BasicProperties(correlation_id=correlation_id)
    )


def build_response_queue(rbmq_address:str, correlation_id:str) -> Tuple[BlockingConnection, BlockingChannel, str, str]:
    """Builds the queue that clients can use to recover the final pipeline
    response once it finishes.
    Returns the new connection, channel, queue name and correlation ID of the request."""

    connection = pika.BlockingConnection(pika.ConnectionParameters(rbmq_address))
    channel = connection.channel()

    result = channel.queue_declare(queue='', exclusive=True, auto_delete=True)
    response_queue:str = result.method.queue

    if not correlation_id:
        # unless explicitly set, we use the queue name (rabbit ensures it is unique)
        # as correlation ID. We also add a timestamp for more entropy
        correlation_id = response_queue + str(time.monotonic())

    # bind the queue to listen to messages posted for our correlation ID
    channel.queue_bind(exchange=CLIENT_RESPONSE_EXCHANGE, queue=response_queue, routing_key=correlation_id)
    return connection, channel, response_queue, correlation_id


def consume_response(channel:BlockingChannel, queue_name:str) -> Generator[str, None, None]:
    """This function should be used by clients that await the pipeline's
    response."""

    generator = channel.consume(queue_name, auto_ack=True)

    # expects 3 messages (one por each result in the output of the pipeline)
    for _ in range(3):
        _, _, body = next(generator)
        yield body.decode('utf-8')


def send_done(channel:BlockingChannel, worker:str, correlation_id:str):
    """Sends a special package indicating the worker has finished sending all
    the data."""

    num_messages = service_config.WORKERS[worker]
    for i in range(num_messages):
        send_data(data=f'\x00{WORKER_TASK}_{WORKER_ID}\x1c{i}', channel=channel, worker=worker, shard_key=i, correlation_id=correlation_id)


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
    def _wrapper(channel:BlockingChannel, worker_id:str):
        # at this point we might be recovering from an unexpected shutdown.
        # we need to check in case we were sending "DONE" messages and were interrupted
        # to do so, we check if we had already reached the expected number of packages
        # from the previous stage.
        active_streams = _get_active_streams()
        for correlation_id, done_messages_received in _load_done_count_from_storage().items():
            print('[RECOVERY] send DONE messages for stream', correlation_id)
            send_done_messages_if_task_is_done(
                channel=channel,
                worker_name=worker_name,
                correlation_id=correlation_id,
                received_ids=done_messages_received,
                seen_messages=None,
                active_streams=active_streams
            )
        
        task_callback(channel, worker_id=WORKER_ID)

    REGISTERED_WORKERS[worker_name] = _wrapper
    return _wrapper


def consume_from(
        channel:BlockingChannel,
        worker_name:str,
        remove_duplicates:bool = False,
        check_as_list=False
    ) -> Generator[Tuple[str, Union[END_OF_STREAM, bytes]], None, None]:
    """Yields messages received from the indicated stage `worker_name` until
    all "DONE" signals are received."""

    # maps correlation IDs to sets of message hashes
    msg_count:Dict[str, int] = defaultdict(int)
    seen_messages:Dict[str, set] = defaultdict(set)
    active_streams = _get_active_streams()

    if worker_name in service_config.SHARDED:
        # sharded stages guarantee that the messages are routed by the worker
        # unique ID. Thus, we use an exclusive queue and a "direct" exchange
        result = channel.queue_declare(queue=STORAGE_ID, exclusive=False, durable=True, auto_delete=False)
        queue_name = result.method.queue

        channel.queue_bind(exchange=worker_name, queue=queue_name, routing_key=WORKER_ID)
    else:
        # normal stages use a named queue as the routing key
        queue_name = worker_name

    done_messages_received = _load_done_count_from_storage()

    # if we are recovering from an unexpected shutdown and this is a stateful
    # node, we need to replay the stream in order to get consistent results
    if remove_duplicates:
        for correlation_id in active_streams:
            print('replaying stream', correlation_id)
            for body in _load_stream(correlation_id=correlation_id):
                # make sure to update the message count for the rest of the stream
                msg_count[correlation_id] += 1
                if check_as_list:
                    body = json.loads(body)
                    for elem in body:
                        mark_message_as_seen(seen_set=seen_messages, msg=json.dumps(elem).encode('utf-8'), correlation_id=correlation_id)
                else:
                    mark_message_as_seen(seen_set=seen_messages, msg=body, correlation_id=correlation_id)

                yield correlation_id, body

                killer.kill_if_applies(stage='during_stream_replay', correlation_id=correlation_id)

    
    # starts consuming events from the queue
    for method_frame, properties, body in channel.consume(queue_name, auto_ack=False):
        cid = properties.correlation_id

        if LOG_MESSAGES:
            file_name = f'/logs/{cid}/input/{STORAGE_ID}.txt'
            if file_name not in LOG_FILES:
                os.makedirs(os.path.dirname(file_name), exist_ok=True)
                LOG_FILES[file_name] = open(file_name, 'wb', buffering=0)

            LOG_FILES[file_name].write(body + b'\n')

        if m := DONE_RE.match(body.decode('utf-8')):
            accepted = _handle_done_message(
                m=m,
                received=done_messages_received,
                correlation_id=cid,
            )

            if accepted:
                if _is_task_done(worker_name, done_messages_received[cid]):
                    # send the EOS message before sending the DONEs because the worker
                    # might send a final package when this message is received
                    yield cid, END_OF_STREAM()

                _update_done_counter(counters=done_messages_received)

                # once we stored the new count we can acknowledge the DONE
                # in RBMQ, because the operation to send DONEs will be retried
                # in case of error
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                killer.kill_if_applies(stage='before_sending_done', correlation_id=cid)

                # now signal the end of the stream to the next stage
                send_done_messages_if_task_is_done(
                    channel=channel,
                    worker_name=worker_name,
                    correlation_id=cid,
                    received_ids=done_messages_received[cid],
                    seen_messages=seen_messages,
                    active_streams=active_streams
                )
            else:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)

            continue

        if cid not in active_streams:
            active_streams.add(cid)
            print('new stream detected', cid)
            _store_active_streams(active_streams)

        if remove_duplicates:
            if check_as_list:
                body = json.loads(body)
                assert isinstance(body, list), type(body)
                delete_list = []
                for i, elem in enumerate(body):
                    if was_msg_seen(seen_set=seen_messages, msg=json.dumps(elem).encode('utf-8'), correlation_id=cid):
                        delete_list.append(i)

                for delete_count, i in enumerate(delete_list):
                    del body[i - delete_count]

                if not body:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    continue
            else:
                if was_msg_seen(seen_set=seen_messages, msg=body, correlation_id=cid):
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    continue

            # store the message in case we need to replay the stream after a crash
            if check_as_list:
                store_msg(data=json.dumps(body).encode('utf-8'), correlation_id=cid, id=msg_count[cid])
                for elem in body:
                    mark_message_as_seen(seen_set=seen_messages, msg=json.dumps(elem).encode('utf-8'), correlation_id=cid)
            else:
                store_msg(data=body, correlation_id=cid, id=msg_count[cid])
                mark_message_as_seen(seen_set=seen_messages, msg=body, correlation_id=cid)

        yield cid, body

        msg_count[cid] += 1

        killer.kill_if_applies(stage='after_msg', correlation_id=cid, msg_count=msg_count[cid])

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def _handle_done_message(m:re.Match, received:Dict[str, list], correlation_id:str):
    sender_id, target_id = m.group(1), m.group(2)

    if target_id != WORKER_ID:
        # this message was meant for another node
        return False
    elif sender_id in received[correlation_id]:
        # reaching this point means that this is a duplicated message, so
        # we can ignore it
        print('received duplicated DONE message', correlation_id, sender_id, target_id)
        return True
    else:
        # the message was not seen before. We need to update the storage
        # to mark it as seen before ACK'ing
        received[correlation_id].append(sender_id)

        print('received DONE message', correlation_id, sender_id, target_id)
        return True


def send_done_messages_if_task_is_done(
        channel,
        worker_name:str,
        correlation_id:str,
        received_ids:List[int],
        seen_messages:Dict[str, Set[bytes]],
        active_streams:Set[str]
    ) -> bool:
    if _done_messages_sent(correlation_id):
        _delete_stream(correlation_id=correlation_id, active_streams=active_streams)
        if seen_messages:
            del seen_messages[correlation_id]

        print('DONE messages were already sent, skipping')
        return False

    if _is_task_done(worker_name, received_ids):
        print('sending DONE messages')

        # propagate the DONE messages to the next step
        for next_task in service_config.NEXT_TASK[worker_name]:
            send_done(channel=channel, worker=next_task, correlation_id=correlation_id)

        _mark_done_messages_as_sent(correlation_id=correlation_id)
        _delete_stream(correlation_id=correlation_id, active_streams=active_streams)
        if seen_messages:
            del seen_messages[correlation_id]

        print('sending DONE messages complete!')
        return True

    # still missing some DONE messages
    return False


def _is_task_done(worker_name:str, done_messages_received:List[str]):
    """Checks if the given worker has already received all the "DONE" messages
    from the previous' stage workers."""

    received = len(set(done_messages_received))
    print(worker_name, 'received', received, 'but expects', WORKERS_TO_WAIT[worker_name])
    return received == WORKERS_TO_WAIT[worker_name]


def _load_done_count_from_storage() -> Dict[str, List[str]]:
    """Load the count about done messages received for each request so far
    from the distributed storage layer. This is required in case the node id recreated
    after an unexpected shutdown."""

    data = storage.read(id=STORAGE_ID, key='done_count')

    counters:Dict[str, List[str]] = defaultdict(list)
    if data:
        persisted_counters = json.loads(data)
        counters.update(persisted_counters)

    return counters


def _update_done_counter(counters:Dict[str, List[int]]):
    """Increments the DONE counter in the persistent storage."""

    data = json.dumps(counters).encode('utf-8')
    storage.set(id=STORAGE_ID, key='done_count', value=data)


def _delete_stream_from_done_counter(correlation_id:str):
    counters = _load_done_count_from_storage()
    counters.pop(correlation_id, None)
    _update_done_counter(counters=counters)


def _done_messages_sent(correlation_id:str) -> bool:
    """Returns whether the DONE messages for the given worker
    and correlation ID were already sent."""

    value = storage.read(id=STORAGE_ID, key=f'done_messages_sent-{correlation_id}')
    return value == b'1'


def _mark_done_messages_as_sent(correlation_id:str):
    """Stores a value indicating that the DONE messages for the given worker
    and correlation ID were sent."""

    storage.set(id=STORAGE_ID, key=f'done_messages_sent-{correlation_id}', value=b'1')
    print('finished stream', correlation_id)


def mark_message_as_seen(seen_set:Dict[str, set], msg:bytes, correlation_id:str):
    m = hashlib.sha256()
    m.update(msg)
    digest = m.digest()
    seen_set[correlation_id].add(digest)


def was_msg_seen(seen_set:Dict[str, set], msg:bytes, correlation_id:str) -> bool:
    m = hashlib.sha256()
    m.update(msg)
    digest = m.digest()
    if digest in seen_set[correlation_id]:
        print('duplicated message detected', correlation_id, digest, msg[:120])
        return True
    return False


def store_msg(data:bytes, correlation_id:str, id:int):
    """Function used to store the stream of messages that a particular
    worker received as input.
    The `id` must be an unique sequential identifier that starts from 0 and has no
    gaps."""

    storage.set(id=STORAGE_ID, key=f'{correlation_id}.{id}', value=data)


def _get_active_streams() -> Set[str]:
    """Returns a list of the correlation IDs associated to client streams that
    were being processed (possibly before an unexpected shutdown)."""

    data = storage.read(id=STORAGE_ID, key=f'correlation_ids')
    if data is None:
        return set()

    ids = json.loads(data)
    assert isinstance(ids, list), ids

    print('found active streams', ids)
    return set(ids)


def _store_active_streams(active_streams:Set[str]):
    """Stores the set of active streams as a list in the storage service."""

    streams_list = list(active_streams)
    data = json.dumps(streams_list).encode('utf-8')
    storage.set(id=STORAGE_ID, key=f'correlation_ids', value=data)

    
def _load_stream(correlation_id:str) -> Generator[bytes, None, None]:
    """Returns a generator that outputs all the messages stored for a particular stream
    (identified by the `correlation_id`) in the original order for the worker calling
    this function."""

    # read incrementing the message ID until we don't find a message in the storage.
    # Once we fail to find a value, we can safely assume the stream ended
    # because there is only one node writing in the same STORAGE_ID at a time
    id = 0
    while True:
        value = storage.read(id=STORAGE_ID, key=f'{correlation_id}.{id}')
        if value is None:
            # this means that there is no message associated to this ID, so
            # the stream finished here
            print('last ID tried when recovering stream', correlation_id, 'was', id)
            return
        
        yield value
        id += 1


def _delete_stream(correlation_id:str, active_streams:Set[str]):
    """Removes stream data from the worker storage. After this function executes
    successfully, the stream will no longer be considered by this worker."""

    print('[ DELETE ] start', correlation_id)

    if correlation_id in active_streams:
        active_streams.remove(correlation_id)
        _store_active_streams(active_streams=active_streams)
    
    # this should be the last operation because after this is done, the stream
    # will be effectively considered deleted by this worker
    _delete_stream_from_done_counter(correlation_id=correlation_id)

    print('[ DELETE ] finished', correlation_id)
