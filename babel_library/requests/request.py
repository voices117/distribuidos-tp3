import os
from babel_library.gatherer import Gatherer
from babel_library.requests.prepare import Prepare
from babel_library.requests.commit import Commit
from babel_library.commons.communication import send_request_to
import babel_library.commons.constants as constants
from babel_library.commons.helpers import intTryParse
from functools import reduce

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

class Request:
    def __init__(self, req):
        if "immediately" in req and req["immediately"] == True:
            self.immediately = True
        else:
            self.immediately = False
        self.gatherer = Gatherer()

    def handle_two_phase_commit(self):
        responses = []

        # Send the PREPARE message with the payload
        prepare = Prepare(self.to_dictionary())
        responses = self.gatherer.gather(prepare)

        # If I have quorum
        ready_received = len(list(filter(lambda r: r["status"] == constants.READY, responses)))
        if ready_received >= QUORUM - 1:
            commit = Commit(prepare.id)
            responses = self.gatherer.gather(commit)

        return responses

    def to_dictionary(self):
        return {
            "type": self.type
        }