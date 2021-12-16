import commons.constants as constants

class Commit():
    def __init__(self, id):
        self.type = constants.COMMIT_REQUEST
        self.id = id
        

    def execute(self, librarian, siblings):
        return librarian.execute_saved_request(self.id)

    def to_dictionary(self):
        return {
            "type": self.type,
            "id": self.id
        }