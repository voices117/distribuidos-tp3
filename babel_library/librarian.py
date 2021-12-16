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

architecture = json.loads(os.environ.get('ARCHITECTURE') or '[{"id": 2, "name": "axel-AB350-Gaming-3", "port": 5001}]') 
siblings = list(filter(lambda l: l["id"] != WORKER_ID, architecture))

class Librarian:
    def __init__(self):
        self.library = Library()
        self.sock = Socket()
        self.saved_requests = {}
        client = self.sock.listen(MAX_QUEUE_SIZE, PORT)
        print(f"Librarian listening on port {PORT}.")

        # Main loop
        while True:
            client = self.sock.attend()
            req = client.receive()
            res = self.handle(req)
            client.send(res)
            client.close()

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

    def save_request(self, req):
        self.saved_requests[req.id] = req.request

    def execute_saved_request(self, id):
        req = self.saved_requests[id]
        req["immediately"] = True
        return self.handle(req)
    
    def sync(self, request, content):
        """This function is called when a READ happens, it tries to sync all the nodes with the
        response of the majority"""
        req = Write({
            "client": request.client,
            "stream": request.stream,
            "payload": content,
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