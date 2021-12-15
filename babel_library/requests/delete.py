from requests.request import Request
import commons.constants as constants

class Delete(Request):
    def __init__(self, req):
        self.type = constants.DELETE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.internal = False if "internal" not in req or req["internal"] is not True else True

    def handle_internal(self, library):
        return library.handle_delete(self)
