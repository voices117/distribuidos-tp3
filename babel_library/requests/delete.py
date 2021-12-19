import babel_library.commons.constants as constants

class Delete():
    def __init__(self, req):
        self.type = constants.DELETE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]

    def handle_internal(self, library):
        library.handle_delete(self)
        return { "status": constants.OK_STATUS }

    def execute(self, librarian):
        print("Executing delete: ", self.to_dictionary())
        librarian.library.handle_delete(self)
        return { "status": constants.OK_STATUS }

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream
        }