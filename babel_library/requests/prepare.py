import commons.constants as constants
import uuid

class Prepare():
    def __init__(self, req):
        self.type = constants.PREPARE_REQUEST

        if req["type"] == constants.PREPARE_REQUEST:
            self.id = req["id"]
            self.request = req["request"]
        else:
            self.id = str(uuid.uuid1())
            self.request = req

    def execute(self, librarian, siblings):
        librarian.save_request(self)
        return { "status": constants.READY }


    def to_dictionary(self):
        return {
            "type": self.type,
            "id": self.id,
            "request": self.request
        }