from babel_library.requests.request import Request
import babel_library.commons.constants as constants

class Delete(Request):
    def __init__(self, req):
        super().__init__(req)
        self.type = constants.DELETE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]

    def handle_internal(self, library):
        library.handle_delete(self)
        return { "status": constants.OK_STATUS }

    def execute(self, librarian):

        if not self.immediately:
            self.handle_two_phase_commit()

        return self.handle_internal(librarian.library)

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "immediately": self.immediately
        }