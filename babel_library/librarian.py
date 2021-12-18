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
from babel_library.commons.helpers import intTryParse
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
        self.sock = Socket()
        self.prepared_requests = {}
        client = self.sock.listen(MAX_QUEUE_SIZE, PORT)
        #self.init_failsafe_storage()
        print(f"Librarian listening on port {PORT}.")
        
        # Main loop
        while True:
            client = self.sock.attend()
            req = client.receive()
            res = self.handle(req)
            client.send(res)
            client.close()

    def init_failsafe_storage(self):
        self.connection = middleware.connect(RABBITMQ_ADDRESS)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=f'storage_{WORKER_ID}')

    def save_action(self, req):
        self.channel.basic_publish(exchange='',
                      routing_key=f'storage_{WORKER_ID}',
                      body=json.dumps(req.to_dictionary()))

    def handle(self, request):
        """Saves/Reads the request to/from it's own storage,
         and dispatches the requests to the other librarians to get quorum"""
        req = self.parse(request)
        return req.execute(self, siblings)
        
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

    def prepare_request(self, req):
        self.prepared_requests[req.id] = req.request

    def execute_prepared_request(self, id):
        if id in self.prepared_requests:
            req = self.prepared_requests[id]
            req["immediately"] = True
            return self.handle(req)

        return { "status": constants.OK_STATUS }
    
    def sync(self, request, content):
        """This function is called when a READ happens, it tries to sync all the nodes with the
        response of the majority"""
        req = Write({
            "client": request.client,
            "stream": request.stream,
            "payload": content.rstrip('\n'),
            "replace": True,
            "immediately": True
        })

        for sibling in siblings:
            try:
                self.dispatch(req, sibling)
            except Exception as e:
                print('Error dispatching prepare', e)


    def dispatch(self, req, sibling):
        """Dispatches the request to siblings to save/read to/from their storage"""
        try:
            res = send_request_to(sibling["name"], sibling["port"], req.to_dictionary(), TIMEOUT)
        except Exception as err:
            raise { "status": constants.ERROR_STATUS, "message": err }
        
        return res