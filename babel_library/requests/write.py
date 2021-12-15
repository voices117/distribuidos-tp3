from requests.request import Request
import commons.constants as constants

class Write(Request):
    def __init__(self, req):
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.payload = req["payload"]
        self.replace = req["replace"]

    def execute(self, librarian, siblings, immediately):

        if not immediately:
            self.handle_two_phase_commit(siblings)

        return super().execute(librarian, siblings, immediately) 
            

    def handle_internal(self, library):
        return library.handle_write(self)
