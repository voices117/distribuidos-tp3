import socket
from commons.socket import Socket
import commons.constants as constants

TIMEOUT = 1 #seconds
PORT = 5111 #TODO: Resolve
HOST = socket.gethostname() #TODO: Resolve

class Borges:
    def __init__(self):
        pass

    def save(self, client, stream, payload):
        s = Socket(TIMEOUT)

        try:
            s.connect(HOST, PORT)
            s.send({
                "type": constants.WRITE_REQUEST,
                "client": client,
                "stream": stream,
                "payload": payload,
            })
            res = s.receive()
            s.close()
        except Exception:
            { "status": constants.CLIENT_ERROR_STATUS, "message": "Error saving" }

        return
    

    def read(self, client, stream):
        s = Socket(TIMEOUT)

        try:
            s.connect(HOST, PORT)
            s.send({
                "type": constants.READ_REQUEST,
                "client": client,
                "stream": stream,
            })
            res = s.receive()
            s.close()
        except Exception:
            { "status": constants.CLIENT_ERROR_STATUS, "message": "Error reading" }
            return


        return res