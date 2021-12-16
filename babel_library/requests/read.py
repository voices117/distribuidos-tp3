import os
from requests.request import Request
import commons.constants as constants
from commons.helpers import intTryParse

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2

class Read(Request):
    def __init__(self, req):
        super().__init__(req)
        self.type = constants.READ_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]


    def handle_internal(self, library):
        return library.handle_read(self)

    def execute(self, librarian, siblings):
        responses = []
        successCount = 0
        
        res = super().execute(librarian, siblings) 
        successCount+=1
        responses.append(res)

        if not self.immediately:
            (responses, successCount) = self.handle_read_propagation(librarian, siblings)

            if successCount >= QUORUM:
                majority_response = format(self.determineResponse(responses))
                librarian.sync(self, majority_response) #TODO: Do in thread
                return majority_response
            else:
                print("Didn't get quorum")
                return { "status": constants.ERROR_STATUS }
        else:
            return res


    def handle_read_propagation(self, librarian, siblings):
        responses = []
        # Send the read request to siblings
        successCount = 0
        self.immediately = True
        for sibling in siblings:
            try:
                res = librarian.dispatch(self, sibling)
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