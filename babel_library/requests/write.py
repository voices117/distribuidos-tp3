from babel_library.requests.request import Request
import babel_library.commons.constants as constants

class Write(Request):
    def __init__(self, req):
        super().__init__(req)
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.payload = req["payload"]
        self.replace = req["replace"]

    def execute(self, librarian):

        if not self.immediately:
            self.handle_two_phase_commit()

        self.immediately = True
        return self.handle_internal(librarian.library) 
            

    def handle_internal(self, library):
        library.handle_write(self)
        return { "status": constants.OK_STATUS }

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "payload": self.payload,
            "replace": self.replace,
            "immediately": self.immediately
        }