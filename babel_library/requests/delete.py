from requests.request import Request
import commons.constants as constants

class Delete(Request):
    def __init__(self, req):
        self.type = constants.DELETE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]

    def handle_internal(self, library):
        return library.handle_delete(self)

    def execute(self, librarian, siblings, immediately):

        if not immediately:
            self.handle_two_phase_commit(siblings)

        return super().execute(librarian, siblings, immediately) 

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream
        }