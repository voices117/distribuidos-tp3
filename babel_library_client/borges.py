import sys

sys.path.insert(0, './babel_library')

import babel_library.commons.constants as constants
from babel_library.commons.communication import send_request_to

class Borges:
    def __init__(self, address, port, timeout=0):
        self.address = address
        self.port = port
        self.timeout = timeout
        pass

    def save(self, client, stream, payload, replace=False):
        try:
            req = {
                "type": constants.WRITE_REQUEST,
                "client": client,
                "stream": stream,
                "payload": payload,
                "replace": replace
            }

            res = send_request_to(self.address, self.port, req, self.timeout)
            print(res)
        except Exception:
            raise { "status": constants.CLIENT_ERROR_STATUS, "message": "Error saving" }
    
        return res

    def read(self, client, stream):
        try:
            req = {
                "type": constants.READ_REQUEST,
                "client": client,
                "stream": stream,
            }
            res = send_request_to(self.address, self.port, req, self.timeout)
        except Exception:
            raise { "status": constants.CLIENT_ERROR_STATUS, "message": "Error reading" }
  
        return res

    def delete(self, client, stream):
        try:
            req = {
                "type": constants.DELETE_REQUEST,
                "client": client,
                "stream": stream,
            }
            res = send_request_to(self.address, self.port, req, self.timeout)
            print(res)
        except Exception:
            raise { "status": constants.CLIENT_ERROR_STATUS, "message": "Error deleting" }
  
        return res