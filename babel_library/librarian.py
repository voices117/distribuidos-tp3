import os
import random
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

architecture = json.loads(os.environ.get('ARCHITECTURE') or "[]") 
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
        req = self.parse(request)
        return req.execute(self.library, siblings)
        
    def parse(self, request):
        if request["type"] == constants.READ_REQUEST:
            return Read(request)
        elif request["type"] == constants.WRITE_REQUEST:
            return Write(request)
        elif request["type"] == constants.DELETE_REQUEST:
            return Delete(request)


    def handle_read(self, request):
        if "internal" not in request or request["internal"] is not True: #This means that the request comes directly from the client
            pass
        else:
            pass
        successCount = 0
        responses = []

        try:
            res = self.library.handle(request)
            responses.append(res)
            successCount += 1
        except Exception as e:
            print('Error', e)

        # Send the request to siblings
        if "internal" not in request or request["internal"] is not True:
            successCount, responses = self.propagate_request(siblings, request)
        else:
            return res

       
        if successCount >= QUORUM :
            return self.determineResponse(responses) # This will compare all responses and return the one with the most appearances
        else:
            print(responses)
            return { "status": constants.ERROR_STATUS }

    def handle_delete(self, request):
        successCount = 0

        try:
            res = self.library.handle(request)
            successCount += 1
        except Exception as e:
            print('Error', e)

        # Send the request to siblings
        if "internal" not in request or request["internal"] is not True:
            successCount, responses = self.propagate_request(siblings, request)
        else:
            return res

        if successCount >= QUORUM:
            return { "status": constants.OK_STATUS }
        else:
            return { "status": constants.ERROR_STATUS }


    def propagate_request(self, siblings, request):
        successCount = 0
        responses = []
        
        for sibling in siblings:
            try:
                print(f'Dispatching to: {sibling}')
                res = self.dispatch(request, sibling)
                print(f'Response: {res}')
                responses.append(res)
                successCount += 1
            except Exception as e:
                print('error dispatching', e)

        return (successCount, responses)


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