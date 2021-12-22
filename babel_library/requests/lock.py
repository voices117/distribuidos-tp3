import babel_library.commons.constants as constants

class Lock():
    def __init__(self, req):
        self.type = constants.LOCK_REQUEST
        self.client = req["client"]
        self.stream = req["stream"]
        self.source = req.get("source")
        self.timeout = req["timeout"]
        
    def execute(self, librarian):
        print(f'Executing lock: client:{self.client}, stream:{self.stream}')
        try:
            librarian.library.handle_lock(self)
            print(f'Lock aquired, client:{self.client} stream:{self.stream}')
            return { "status": constants.OK_STATUS }
        except Exception as err:
            return { "status": constants.ERROR_STATUS, "message": str(err) }

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
        }