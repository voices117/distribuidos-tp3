import sys

sys.path.insert(0, './babel_library')

import babel_library.commons.constants as constants
from babel_library.commons.communication import send_request_to
from service_config import LIBRARIANS
import random

LIBRARIANS = [
    {
        "id": 1,
        "name": "173.150.125.2",
        "port": 5000
    }
]

class Borges:
    def __init__(self, timeout=0):
        self.timeout = timeout
        self.provider = {}
        self.repick_provider()

    def repick_provider(self):
        rand = random.randint(0, len(LIBRARIANS) - 1)
        self.provider = LIBRARIANS[rand]

    def save(self, client, stream, payload, replace=False):
        for i in range(0, 3):
            try:
                req = {
                    "type": constants.WRITE_REQUEST,
                    "client": client,
                    "stream": stream,
                    "payload": payload,
                    "replace": replace
                }

                res = send_request_to(self.provider["name"], self.provider["port"], req, self.timeout)
                return res
            except Exception as err:
                print(err)
                self.repick_provider()

        return { "status": constants.CLIENT_ERROR_STATUS, "message": "Error saving" }

    def read(self, client, stream):
        for i in range(0, 3):
            try:
                req = {
                    "type": constants.READ_REQUEST,
                    "client": client,
                    "stream": stream,
                }
                res = send_request_to(self.provider["name"], self.provider["port"], req, self.timeout)
                return res
            except Exception as err:
                print(err)
                self.repick_provider()

        return { "status": constants.CLIENT_ERROR_STATUS, "message": "Error reading" }


    def delete(self, client, stream):
        for i in range(0, 3):
            try:
                req = {
                    "type": constants.DELETE_REQUEST,
                    "client": client,
                    "stream": stream,
                }
                res = send_request_to(self.provider["name"], self.provider["port"], req, self.timeout)
                return res
            except Exception as err:
                print(err)
                self.repick_provider()

        return { "status": constants.CLIENT_ERROR_STATUS, "message": "Error deleting" }
