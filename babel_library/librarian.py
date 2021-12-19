import os
import random
from babel_library.requests.commit import Commit
from babel_library.requests.prepare import Prepare
from babel_library.requests.delete import Delete
from babel_library.requests.write import Write
from babel_library.requests.read import Read
from babel_library.library import Library
from babel_library.commons.socket import Socket
from babel_library.commons.communication import send_request_to
from babel_library.commons.helpers import intTryParse, tryParse
import babel_library.commons.constants as constants
import json
import middleware
import pika

MAX_QUEUE_SIZE = intTryParse(os.environ.get('MAX_QUEUE_SIZE')) or 5
PORT = intTryParse(os.environ.get('PORT')) or random.randint(5000, 6000)
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds
QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1
RABBITMQ_ADDRESS = os.environ.get('RABBITMQ_ADDRESS') or 'localhost'

architecture = json.loads(os.environ.get('ARCHITECTURE') or '[{"id": 2, "name": "axel-AB350-Gaming-3", "port": 5001}]') 
siblings = list(filter(lambda l: l["id"] != WORKER_ID, architecture))

class Librarian:
    def __init__(self):
        self.library = Library()
        self.recover()

        self.init_input_queue()
        print(f"Librarian ready.")

    def handle(self, ch, method, properties, body):
        """Saves/Reads the request to/from it's own storage,
         and dispatches the requests to the other librarians to get quorum"""
        request = tryParse(body)
        req = self.parse(request)
        res = req.execute(self)

        self.respond(res)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        
    def respond(self, response):
        pass

    def parse(self, request):
        if request["type"] == constants.READ_REQUEST:
            return Read(request)
        elif request["type"] == constants.WRITE_REQUEST:
            return Write(request)
        elif request["type"] == constants.DELETE_REQUEST:
            return Delete(request)
        elif request["type"] == constants.PREPARE_REQUEST:
            return Prepare(request)
        elif request["type"] == constants.COMMIT_REQUEST:
            return Commit(request["id"])
    
    def sync(self, request, content):
        """This function is called when a READ happens, it tries to sync all the nodes with the
        response of the majority"""
        req = Write({
            "client": request.client,
            "stream": request.stream,
            "payload": content.rstrip('\n'),
            "replace": True,
        })

        for sibling in siblings:
            try:
                return send_request_to(sibling["name"], sibling["port"], req.to_dictionary(), TIMEOUT)
            except Exception as e:
                print('Error dispatching prepare', e)

    def recover(self):
        """This method will query all the existing data of the rest of the nodes"""
        """And then sync with that"""
        req = Read({
            "metadata": True
        })
        logs = req.execute(self) #Recover the file tree
        # Make a read request for each one
        logs = tryParse(logs)
        print(logs)
        for log in logs:
            print("Retrieving log: ", log)
            req = Read({ "client": intTryParse(log["client"]), "stream": intTryParse(log["stream"]) })
            res = req.execute(self)
            print(res)
            req = Write({ "client": log["client"], "stream": log["stream"], "payload": res.rstrip('\n'), "replace": True })
            req.execute(self)

    def init_input_queue(self):
        """This method will initialize the input request queue for this worker"""
        """AUTO_ACK is disabled, since the ack will be given after responding to the request"""
        """A single fanout exchange will be responsible for delivering the requests to each of the storage nodes"""
        self.connection = middleware.connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='storage', exchange_type='fanout')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='storage', queue=queue_name)

        self.channel.basic_consume(queue=queue_name, on_message_callback=self.handle, auto_ack=False)
        self.channel.start_consuming()
