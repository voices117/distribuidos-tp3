import babel_library.commons.constants as constants

class Write():
    def __init__(self, req):
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.payload = req["payload"]
        self.replace = req["replace"]

    def execute(self, librarian):
        print("Executing write: ", self.to_dictionary())
        librarian.library.handle_write(self)
        return { "status": constants.OK_STATUS }
            

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "payload": self.payload,
            "replace": self.replace
        }