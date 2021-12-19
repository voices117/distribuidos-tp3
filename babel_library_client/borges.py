import sys
from time import sleep
import pika
import os
import json
import uuid
from babel_library.commons.helpers import intTryParse, tryParse
sys.path.insert(0, './babel_library')

import babel_library.commons.constants as constants
from babel_library.commons.communication import send_request_to
from service_config import LIBRARIANS
import random
import middleware

RABBITMQ_ADDRESS = os.environ.get('RABBITMQ_ADDRESS') or 'localhost'

class Borges:
    def __init__(self, timeout=0):
        self.timeout = timeout
        self.responses = {}
        self.init_storage_queues()
        
    def init_storage_queues(self):
        self.connection = middleware.connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='storage', exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        corr_id = props.correlation_id
        self.responses[corr_id] = tryParse(body)

    def save(self, client, stream, payload, replace=False):
        corr_id = self.get_correlation_id()
        req = {
            "type": constants.WRITE_REQUEST,
            "client": client,
            "stream": stream,
            "payload": payload,
            "replace": replace
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req), 
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=corr_id)
        )

        while corr_id not in self.responses:
            self.connection.process_data_events()

        return self.responses[corr_id]

    def read(self, client, stream):
        corr_id = self.get_correlation_id()
        req = {
            "type": constants.READ_REQUEST,
            "client": client,
            "stream": stream,
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req),
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=corr_id)
        )

        while corr_id not in self.responses:
            self.connection.process_data_events()

        return self.responses[corr_id]

    def delete(self, client, stream):
        corr_id = self.get_correlation_id()
        req = {
            "type": constants.DELETE_REQUEST,
            "client": client,
            "stream": stream,
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req),
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=corr_id)
        )

        while corr_id not in self.responses:
            self.connection.process_data_events()

        return self.responses[corr_id]

    def get_correlation_id(self):
        return str(uuid.uuid4())