
import os
import random
from library import WORKER_ID, Library
from commons.socket import Socket
from commons.communication import send_request_to
from commons.helpers import intTryParse
import commons.constants as constants
import socket
import json

MAX_QUEUE_SIZE = intTryParse(os.environ.get('MAX_QUEUE_SIZE')) or 5
PORT = intTryParse(os.environ.get('PORT')) or random.randint(5000, 6000)
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds
QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

architecture = json.loads(os.environ.get('ARCHITECTURE')) or []
siblings = list(filter(lambda l: l["id"] != WORKER_ID, architecture))


class Librarian:
    def __init__(self):
        self.library = Library()
        self.sock = Socket()
        client = self.sock.listen(MAX_QUEUE_SIZE, PORT)
        print(f"Librarian listening on port {PORT}.")
        
        while True:
            client = self.sock.attend()
            req = client.receive()
            res = self.handle(req)
            client.send(res)
            client.close()

    def handle(self, request):
        """Saves/Reads the request to/from it's own storage,
         and dispatches the requests to the other librarians to get quorum"""
        successCount = 0

        res = self.library.handle(request) # Save it on my own storage
        successCount += 1

        # Send the request to siblings
        if "internal" not in request or request["internal"] is not True:
            for sibling in siblings:
                try:
                    self.dispatch(request, sibling)
                    successCount += 1
                except Exception:
                    print('error dispatching')

        # Only for writes
        if successCount >= QUORUM:
            return { "status": constants.OK_STATUS }
        else:
            return { "status": constants.ERROR_STATUS }


    def dispatch(self, request, sibling):
        """Dispatches the request to siblings to save/read to/from their storage"""
        try:
            request["internal"] = True
            res = send_request_to(sibling["name"], sibling["port"], request, TIMEOUT)
        except Exception:
            raise { "status": constants.INTERNAL_REQUEST_ERROR, "message": "Error saving on replica" }
        
        return res