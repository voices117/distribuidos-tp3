import time
import pika
import os
import json
from babel_library.commons.helpers import tryParse, get_correlation_id
import babel_library.commons.constants as constants
import middleware

RABBITMQ_ADDRESS = os.environ.get('RABBITMQ_ADDRESS') or 'localhost'

class Borges:
    def __init__(self, timeout=2):
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

    def save(self, client, stream, payload):
        req = {
            "type": constants.WRITE_REQUEST,
            "client": client,
            "stream": stream,
            "payload": payload,
            "replace": True
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
