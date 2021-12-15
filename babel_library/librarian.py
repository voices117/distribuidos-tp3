import os
import random
from requests.commit import Commit
from requests.prepare import Prepare
from requests.delete import Delete
from requests.write import Write
from requests.read import Read
from library import Library
from commons.socket import Socket
from commons.communication import send_request_to
from commons.helpers import intTryParse
import commons.constants as constants
import json

MAX_QUEUE_SIZE = intTryParse(os.environ.get('MAX_QUEUE_SIZE')) or 5
PORT = intTryParse(os.environ.get('PORT')) or random.randint(5000, 6000)
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds
QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

architecture = json.loads(os.environ.get('ARCHITECTURE') or '[{"id": 2, "name": "axel-AB350-Gaming-3", "port": 5481}]') 
siblings = list(filter(lambda l: l["id"] != WORKER_ID, architecture))

class Librarian:
    def __init__(self):
        self.library = Library()
        self.sock = Socket()
        self.saved_requests = {}
        client = self.sock.listen(MAX_QUEUE_SIZE, PORT)
        print(f"Librarian listening on port {PORT}.")
        
        while True:
            client = self.sock.attend()
            req = client.receive()
            res = self.handle(req, False)
            client.send(res)
            client.close()

    def handle(self, request, immediately=False):
        """Saves/Reads the request to/from it's own storage,
         and dispatches the requests to the other librarians to get quorum"""
        req = self.parse(request)
        return req.execute(self, siblings, immediately)
        
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

    def save_request(self, req):
        self.saved_requests[req.id] = req.request

    def execute_saved_request(self, id):
        req = self.saved_requests[id]
        return self.handle(req, True)