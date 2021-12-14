import os
from commons.communication import send_request_to
import commons.constants as constants
from commons.helpers import intTryParse

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

class Request:
    def __init__(self):
        self.internal = False

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

    def execute(self, library, siblings):
        responses = []
        # if not self.internal:
        #     responses = self.handle_propagation(siblings)

        
        try:
            responses.append(self.handle_internal(library))
        except Exception as e:
            print('Error', e)
            
        return responses


        # successCount = 0

        # try:
        #     res = library.handle_write(self) # Save it on my own storage
        #     successCount += 1
        # except Exception as e:
        #     print('Error', e)

        # # Send the request to siblings
        # if not self.internal:
        #     successCount, responses = self.propagate_request(siblings, self.to_dictionary())
        # else:
        #     return res

        # if successCount >= QUORUM:
        #     return { "status": constants.OK_STATUS }
        # else:
        #     return { "status": constants.ERROR_STATUS }

    def handle_internal(self):
        pass

    def handle_propagation(self, siblings):
        responses = []

        for sibling in siblings:
            try:
                print(f'Dispatching to: {sibling}')
                res = self.dispatch(self, sibling)
                print(f'Response: {res}')
                responses.append(res)
            except Exception as e:
                responses.append(e)
                print('error dispatching', e)

        return responses

    def dispatch(self, sibling):
        """Dispatches the request to siblings to save/read to/from their storage"""
        try:
            self.internal = True
            res = send_request_to(sibling["name"], sibling["port"], self.to_dictionary(), TIMEOUT)
        except Exception as err:
            print(err)
            raise { "status": constants.INTERNAL_REQUEST_ERROR, "message": "Error saving on replica" }
        
        return res

    def to_dictionary(self):
        return {
            "client": self.client,
            "stream": self.stream,
            "payload": self.payload,
            "internal": self.internal
        }