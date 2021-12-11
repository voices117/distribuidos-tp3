import commons.constants as constants
from commons.communication import send_request_to

class Borges:
    def __init__(self, address, port, timeout=0):
        self.address = address
        self.port = port
        self.timeout = timeout
        pass

    def save(self, client, stream, payload):
        try:
            req = {
                "type": constants.WRITE_REQUEST,
                "client": client,
                "stream": stream,
                "payload": payload,
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
            print(res)
        except Exception:
            raise { "status": constants.CLIENT_ERROR_STATUS, "message": "Error reading" }
  
        return res