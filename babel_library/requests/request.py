import os
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

    def execute(self, librarian, siblings):
        try:
            return self.handle_internal(librarian.library)
        except Exception as err:
            raise { "status": constants.ERROR_STATUS, "message": err }
            

    def handle_internal(self):
        pass

    def handle_two_phase_commit(self, librarian, siblings):
        responses = []

        # Send the PREPARE message with the payload
        prepare = Prepare(self.to_dictionary())
        for sibling in siblings:
            try:
                res = librarian.dispatch(prepare, sibling)
                responses.append(res)
            except Exception as e:
                responses.append(e)
                print('Error dispatching prepare', e)

        # If I have quorum
        ready_received = len(list(filter(lambda r: r["status"] == constants.READY, responses)))
        if ready_received >= QUORUM - 1:
            commit = Commit(prepare.id)
            # Send the commit message
            for sibling in siblings:
                try:
                    res = librarian.dispatch(commit, sibling)
                except Exception as e:
                    print('Error dispatching commit', e)

        return responses

    def to_dictionary(self):
        return {
            "type": self.type
        }