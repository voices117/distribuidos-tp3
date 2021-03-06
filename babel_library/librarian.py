import os
from babel_library.requests.delete import Delete
from babel_library.requests.write import Write
from babel_library.requests.read import Read
from babel_library.requests.lock import Lock
from babel_library.requests.unlock import Unlock
from babel_library.library import Library
from babel_library_client.borges import Borges
from babel_library.commons.helpers import intTryParse, tryParse
from services import killer
import babel_library.commons.constants as constants
import json
import middleware
import pika

WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1
RABBITMQ_ADDRESS = os.environ.get('RABBITMQ_ADDRESS') or 'localhost'

class Librarian:
    def __init__(self):
        self.read_counter = 0
        self.write_counter = 0
        self.library = Library()
        self.recover()

        self.init_rabbit()
        self.init_action_input_queue()
        self.channel.start_consuming()
        print(f"Librarian ready...")

    def handle(self, ch, method, properties, body):
        """Saves/Reads the request to/from it's own storage,
         and dispatches the requests to the other librarians to get quorum"""
        request = tryParse(body)
        req = self.parse(request)

        killer.kill_if_applies("handling_read_request", read_counter=self.read_counter)
        killer.kill_if_applies("handling_write_request", write_counter=self.write_counter)

        if req.source != WORKER_ID:
            res = req.execute(self)
            self.respond(res, properties)
            self.channel.basic_ack(delivery_tag=method.delivery_tag)

        if req.source == WORKER_ID and req.type == constants.WRITE_REQUEST:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        
    def respond(self, response, props):
        """Return a response to the client that requested it through the queue provided in the request"""
        self.channel.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=json.dumps(response))

    def parse(self, request):
        if request["type"] == constants.READ_REQUEST:
            self.read_counter+=1
            return Read(request)
        elif request["type"] == constants.WRITE_REQUEST:
            self.write_counter+=1
            return Write(request)
        elif request["type"] == constants.DELETE_REQUEST:
            return Delete(request)
        elif request["type"] == constants.LOCK_REQUEST:
            return Lock(request)
        elif request["type"] == constants.UNLOCK_REQUEST:
            return Unlock(request)

    def recover(self):
        """This method will create a request to retrieve the existing files on the system and sync with that"""
        req = Read({
            "metadata": True
        })
        client = Borges()
        response = client.execute(req.to_dictionary()) # Recover the file tree
        
        if response["status"] != constants.OK_STATUS:
            print("Storage node could not recover data")
            return

        # Make a read request for each one
        logs = tryParse(response["message"])
        for log in logs:
            print("Retrieving log: ", log)
            req = Read({ "client": log["client"], "stream": log["stream"], "source": WORKER_ID })
            res = client.execute(req.to_dictionary())
            
            req = Write({ "client": log["client"], "stream": log["stream"], "payload": res["message"].rstrip('\n'), "source": WORKER_ID, "replace": True })
            req.execute(self)

    def init_rabbit(self):
        self.connection = middleware.connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

    def init_action_input_queue(self):
        """This method will initialize the input request queue for this worker"""
        """AUTO_ACK is disabled, since the ack will be given after responding to the request"""
        """A single fanout exchange will be responsible for delivering the requests to each of the storage nodes"""
        self.channel.exchange_declare(exchange='storage', exchange_type='fanout')
        result = self.channel.queue_declare(queue=f'storage_{WORKER_ID}')
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='storage', queue=queue_name)

        self.channel.basic_consume(queue=queue_name, on_message_callback=self.handle, auto_ack=False)