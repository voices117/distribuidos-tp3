import babel_library.commons.constants as constants

class Commit():
    def __init__(self, req):
        self.type = constants.COMMIT_REQUEST
        self.id = req["id"]
        
    def execute(self, librarian, siblings):
        return librarian.execute_prepared_request(self.id)

    def to_dictionary(self):
        return {
            "type": self.type,
            "id": self.id
        }