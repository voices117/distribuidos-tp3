from requests.request import Request
import commons.constants as constants

class Read(Request):
    def __init__(self, req):
        self.type = constants.READ_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]

    def handle_internal(self, library):
        return library.handle_read(self)
