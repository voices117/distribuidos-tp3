import babel_library.commons.constants as constants

class Unlock():
    def __init__(self, req):
        self.type = constants.WRITE_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.source = req.get("source")

    def execute(self, librarian):
        print(f'Executing unlock: client:{self.client}, stream:{self.stream}')
        try:
            librarian.library.handle_unlock(self)
            print(f'Lock release, client:{self.client} stream:{self.stream}')
            return { "status": constants.OK_STATUS }
        except Exception as err:
            return { "status": constants.ERROR_STATUS, "message": 'Lock is already free' }
            

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
        }