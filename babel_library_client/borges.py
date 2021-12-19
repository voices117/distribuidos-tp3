import sys
from time import sleep
import pika
import os
import json

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

        self.init_storage_queue()
        
    def init_storage_queue(self):
        self.connection = middleware.connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='storage', exchange_type='fanout')


    def save(self, client, stream, payload, replace=False):
        req = {
            "type": constants.WRITE_REQUEST,
            "client": client,
            "stream": stream,
            "payload": payload,
            "replace": replace
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req))

    def read(self, client, stream):
        req = {
            "type": constants.READ_REQUEST,
            "client": client,
            "stream": stream,
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req))

    def delete(self, client, stream):
        req = {
            "type": constants.DELETE_REQUEST,
            "client": client,
            "stream": stream,
        }
        self.channel.basic_publish(exchange='storage', routing_key='', body=json.dumps(req))