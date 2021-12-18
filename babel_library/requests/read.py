import os
from babel_library.gatherer import Gatherer
from babel_library.requests.request import Request
import babel_library.commons.constants as constants
from babel_library.commons.helpers import intTryParse
from babel_library.commons.communication import send_request_to

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

class Read(Request):
    def __init__(self, req):
        super().__init__(req)
        self.type = constants.READ_REQUEST
        self.client = req.get("client")
        self.stream = req.get("stream")
        self.metadata = req.get('metadata')

    def handle_internal(self, library):
        if self.metadata:
            return library.list_files()
        else:
            return library.handle_read(self)

    def execute(self, librarian):
        successCount = 0
        
        try:
            res = self.handle_internal(librarian.library)
            successCount+=1

            if not self.immediately:
                self.immediately = True
                responses = self.gatherer.gather(self)
                successCount += len(responses)
                responses.append(res)

                if successCount >= QUORUM:
                    majority_response = self.determineResponse(responses)
                    return majority_response
                else:
                    print("Didn't get quorum")
                    return { "status": constants.ERROR_STATUS }
            else:
                return res
        except Exception as err:
            print(f'Error on read {str(err)}')
            return { "status": constants.ERROR_STATUS, "message": str(err)}


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
            "immediately": self.immediately,
            "metadata": self.metadata
        }