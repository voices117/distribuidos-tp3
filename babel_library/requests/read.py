import babel_library.commons.constants as constants
from babel_library.commons.helpers import intTryParse

class Read():
    def __init__(self, req):
        self.type = constants.READ_REQUEST
        self.client = req.get("client")
        self.stream = req.get("stream")
        self.metadata = req.get("metadata")
        self.source = req.get("source")

    def execute(self, librarian):
        print(f'Executing read: client:{self.client} stream: {self.stream}')
        try:
            if self.metadata:
                return { "status": constants.OK_STATUS, "message": librarian.library.list_files() }
            else:
                return { "status": constants.OK_STATUS, "message": librarian.library.handle_read(self) }
        except Exception as err:
            return { "status": constants.ERROR_STATUS, "message": str(err) }

    def to_dictionary(self):
        return {
            "type": self.type,
            "client": self.client,
            "stream": self.stream,
            "metadata": self.metadata
        }