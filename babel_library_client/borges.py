import time
import datetime
import pika
import os
import json
from babel_library.commons.helpers import tryParse, get_correlation_id
import babel_library.commons.constants as constants
import logging
import datetime

RABBITMQ_ADDRESS = os.environ.get('RABBITMQ_ADDRESS') or 'localhost'

def connect(address:str, retries:int = 25):
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

class Borges:
    def __init__(self, timeout=3):
        self.timeout = timeout
        self.responses = {}
        self.init_rabbit()
        self.init_callback_queue()
        self.init_read_storage_queues()
        self.init_action_storage_queues()

    def init_rabbit(self):
        self.connection = connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        
    def init_callback_queue(self):
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def init_action_storage_queues(self):
        """This will initialize the exchange and queue that the client will use to send request to and receive responses
        from the storage servers for the WRITE and DELETE requests"""
        self.channel.exchange_declare(exchange='storage_actions', exchange_type='fanout')

    def init_read_storage_queues(self):
        """This will initialize the exchange and queue that the client will use to send request to and receive responses
        from the storage server for the READ request"""
        self.channel.queue_declare(queue='reads_queue', durable=True)


    def on_response(self, ch, method, props, body):
        corr_id = props.correlation_id

        res = tryParse(body)
        res["timetolive"] = datetime.datetime.utcnow() + datetime.timedelta(seconds=5) # The entry will clear itself after this time
        self.responses[corr_id] = res

        self.clear_responses()

    def clear_responses(self):
        toremove = []
        currenttime = datetime.datetime.utcnow()
        for key, value in self.responses.items():
            if value["timetolive"] < currenttime:
                toremove.append(key)

        for key in toremove:
            del self.responses[key]

    def save(self, client, stream, payload):
        req = {
            "type": constants.WRITE_REQUEST,
            "client": client,
            "stream": stream,
            "payload": payload,
            "replace": True
        }
        return self.execute(req)

    def try_lock(self, client, stream):
        timeout = (datetime.datetime.utcnow() + datetime.timedelta(seconds=5)).isoformat()
        req = {
            "type": constants.LOCK_REQUEST,
            "client": client,
            "stream": stream,
            "timeout": timeout
        }
        return self.execute(req)

    def unlock(self, client, stream):
        req = {
            "type": constants.UNLOCK_REQUEST,
            "client": client,
            "stream": stream
        }
        return self.execute(req)

    def read(self, client, stream):
        req = {
            "type": constants.READ_REQUEST,
            "client": client,
            "stream": stream
        }
        return self.execute(req)

    def delete(self, client, stream):
        req = {
            "type": constants.DELETE_REQUEST,
            "client": client,
            "stream": stream,
        }
        return self.execute(req)


    def execute(self, req):
        corr_id = get_correlation_id()

        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req),
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=corr_id)
        )

        return self.wait_for_response(corr_id)
        
    def wait_for_response(self, corr_id):
        start = time.time()
        while corr_id not in self.responses and time.time() - start < self.timeout:
            self.connection.process_data_events()

        if corr_id not in self.responses:
            return { "status": constants.ERROR_STATUS, "message": "Storage did not respond, please try again" }
            
        response = self.responses[corr_id]
        del self.responses[corr_id]
        return response
