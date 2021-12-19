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

    def execute(self, librarian):
        print("Executing read: ", self.to_dictionary())
        if self.metadata:
            return librarian.library.list_files()
        else:
            return librarian.library.handle_read(self)

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "metadata": self.metadata
        }