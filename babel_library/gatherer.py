from babel_library.commons.communication import send_request_to
import json
import os
from babel_library.commons.helpers import intTryParse
import babel_library.commons.constants as constants


WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1
architecture = json.loads(os.environ.get('ARCHITECTURE') or '[{"id": 2, "name": "axel-AB350-Gaming-3", "port": 5001}]') 
siblings = list(filter(lambda l: l["id"] != WORKER_ID, architecture))
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

"""This class will be in charge of contacting the rest of the librarians and gathering their responses"""
class Gatherer:
    def __init__(self):
        pass

    def gather (self, request):
        responses = []
        for sibling in siblings:
            try:
                res = send_request_to(sibling["name"], sibling["port"], request.to_dictionary(), TIMEOUT)
                responses.append(res)
            except Exception as e:
                print('Error dispatching', e)

        return responses