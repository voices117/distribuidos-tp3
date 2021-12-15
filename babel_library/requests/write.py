from requests.request import Request
import commons.constants as constants

class Write(Request):
    def __init__(self, req):
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.payload = req["payload"]
        self.replace = req["replace"]
        self.internal = False if "internal" not in req or req["internal"] is not True else True

    def handle_internal(self, library):
        return library.handle_write(self)

    # def execute(self, library):
    #     successCount = 0

    #     try:
    #         res = library.handle_write(self) # Save it on my own storage
    #         successCount += 1
    #     except Exception as e:
    #         print('Error', e)

    #     # Send the request to siblings
    #     if not self.internal:
    #         successCount, responses = self.propagate_request(siblings, self.to_dictionary())
    #     else:
    #         return res

    #     if successCount >= QUORUM:
    #         return { "status": constants.OK_STATUS }
    #     else:
    #         return { "status": constants.ERROR_STATUS }
