from requests.request import Request
import commons.constants as constants

class Write(Request):
    def __init__(self, req):
        super().__init__(req)
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.payload = req["payload"]
        self.replace = req["replace"]

    def execute(self, librarian, siblings):

        if not self.immediately:
            self.handle_two_phase_commit(librarian, siblings)

        self.immediately = True
        return super().execute(librarian, siblings) 
            

    def handle_internal(self, library):
        return library.handle_write(self)

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "payload": self.payload,
            "replace": self.replace,
            "immediately": self.immediately
        }