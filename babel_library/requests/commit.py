from requests.request import Request
import commons.constants as constants

class Commit():
    def __init__(self, prepare):
        self.id = prepare.id
        self.type = constants.COMMIT_REQUEST

    def execute(self, librarian, siblings):
        return librarian.execute_saved_request(self.id)
