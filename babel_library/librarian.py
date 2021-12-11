
import os
import random
from library import Library
from commons.socket import Socket
from commons.communication import send_request_to
import commons.constants as constants
import socket

MAX_QUEUE_SIZE = os.environ.get('MAX_QUEUE_SIZE') or 5
PORT = os.environ.get('PORT') or random.randint(5000, 6000)
TIMEOUT = 1 #seconds
QUORUM = 2
siblings = [
    {
        "address": socket.gethostname(),
        "port": 5799
    }
]


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
            res = send_request_to(sibling["address"], sibling["port"], request, TIMEOUT)
        except Exception:
            raise { "status": constants.INTERNAL_REQUEST_ERROR, "message": "Error saving on replica" }
        
        return res