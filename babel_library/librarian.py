
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
        responses = []

        try:
            res = self.library.handle(request) # Save it on my own storage
            responses.append(res)
            successCount += 1
        except Exception as e:
            print('Error', e)

        # Send the request to siblings
        if "internal" not in request or request["internal"] is not True:
            for sibling in siblings:
                try:
                    print(f'Dispatching to: {sibling}')
                    res = self.dispatch(request, sibling)
                    print(f'Response: {res}')
                    responses.append(res)
                    successCount += 1
                except Exception as e:
                    print('error dispatching', e)
        else:
            return res


        # Only for writes
        if successCount >= QUORUM and request["type"] == constants.WRITE_REQUEST:
            return { "status": constants.OK_STATUS }
        elif successCount >= QUORUM and request["type"] == constants.READ_REQUEST:
            return self.determineResponse(responses) # This will compare all responses and return the one with the most appearances
        else:
            print(responses)
            return { "status": constants.ERROR_STATUS }


    def dispatch(self, request, sibling):
        """Dispatches the request to siblings to save/read to/from their storage"""
        try:
            request["internal"] = True
            res = send_request_to(sibling["name"], sibling["port"], request, TIMEOUT)
        except Exception as err:
            print(err)
            raise { "status": constants.INTERNAL_REQUEST_ERROR, "message": "Error saving on replica" }
        
        return res

    def determineResponse(self, responses):
        """Returns the most frecuent response"""
        dict = {}
        count, itm = 0, ''
        for item in responses:
            res = str(item)

            dict[res] = dict.get(res, 0) + 1
            if dict[res] >= count:
                count, itm = dict[res], item
        return itm