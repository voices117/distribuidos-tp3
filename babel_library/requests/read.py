from requests.request import Request
import commons.constants as constants

class Read(Request):
    def __init__(self, req):
        self.client = req["client"]
        self.stream = req["stream"]
        self.internal = False if "internal" not in req or req["internal"] is not True else True

    def handle_internal(self, library):
        return library.handle_read(self)
