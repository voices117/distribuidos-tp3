from requests.request import Request
import commons.constants as constants
import uuid

class Prepare():
    def __init__(self, req):
        self.type = constants.PREPARE_REQUEST
        self.id = uuid.uuid1()
        self.req = req


    def execute(self, librarian, siblings):
        librarian.save_request(self)
        return { "status": constants.READY }


    def to_dictionary(self):
        return {
            "type": self.type,
            "id": self.id,
            "req": self.req
        }