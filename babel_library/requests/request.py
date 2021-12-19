import os
from babel_library.gatherer import Gatherer
from babel_library.commons.communication import send_request_to
import babel_library.commons.constants as constants
from babel_library.commons.helpers import intTryParse
from functools import reduce

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

class Request:
    def __init__(self, req):
        self.gatherer = Gatherer()

    def to_dictionary(self):
        return {
            "type": self.type
        }