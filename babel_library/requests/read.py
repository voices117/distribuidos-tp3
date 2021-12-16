import os
from requests.request import Request
import commons.constants as constants
from commons.helpers import intTryParse

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2

class Read(Request):
    def __init__(self, req):
        self.type = constants.READ_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]

        if "immediately" in req:
            self.immediately = True
        else:
            self.immediately = False

    def handle_internal(self, library):
        return library.handle_read(self)

    def execute(self, librarian, siblings, immediately):
        responses = []
        successCount = 0
        
        res = super().execute(librarian, siblings, immediately) 
        successCount+=1
        responses.append(res)    

        if not self.immediately:
            (responses, successCount) = self.handle_read_propagation(siblings)

            if successCount >= QUORUM:
                return self.determineResponse(responses)
            else:
                print("Didn't get quorum")
                return { "status": constants.ERROR_STATUS }
        else:
            return res


    def handle_read_propagation(self, siblings):
        responses = []
        # Send the read request to siblings
        successCount = 0
        for sibling in siblings:
            try:
                res = self.dispatch(self, sibling)
                responses.append(res)
                successCount += 1
            except Exception as e:
                print('error dispatching', e)

        return (responses, successCount)


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

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "immediately": self.immediately
        }