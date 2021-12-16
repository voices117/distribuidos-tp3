import os
from requests.prepare import Prepare
from requests.commit import Commit
from commons.communication import send_request_to
import commons.constants as constants
from commons.helpers import intTryParse
from functools import reduce

QUORUM = intTryParse(os.environ.get('QUORUM')) or 2
TIMEOUT = intTryParse(os.environ.get('TIMEOUT')) or 1 #seconds

class Request:
    def __init__(self):
        pass

    def execute(self, librarian, siblings, immediately):
        try:
            return self.handle_internal(librarian.library)
        except Exception as err:
            raise { "status": constants.ERROR_STATUS, "message": err }
            

    def handle_internal(self):
        pass

    def handle_two_phase_commit(self, siblings):
        responses = []

        # Send the PREPARE message with the payload
        prepare = Prepare(self.to_dictionary())
        for sibling in siblings:
            try:
                print(f'Sending prepare to: {sibling}')
                res = self.dispatch(prepare, sibling)
                print(f'Response: {res}')
                responses.append(res)
            except Exception as e:
                responses.append(e)
                print('Error dispatching prepare', e)


        # If I have quorum
        ready_received = len(list(filter(lambda r: r["status"] == constants.READY, responses)))
        if ready_received >= QUORUM - 1:
            # TODO: Save Request commit to rabbit
            
            # Send the commit message
            for sibling in siblings:
                try:
                    print(f'Sending commit to: {sibling}')
                    commit = Commit(prepare.id)
                    res = self.dispatch(commit, sibling)
                    print(f'Response: {res}')
                except Exception as e:
                    print('Error dispatching commit', e)

        return responses


    def dispatch(self, req, sibling):
        """Dispatches the request to siblings to save/read to/from their storage"""
        try:
            res = send_request_to(sibling["name"], sibling["port"], req.to_dictionary(), TIMEOUT)
        except Exception as err:
            raise { "status": constants.ERROR_STATUS, "message": err }
        
        return res

    def to_dictionary(self):
        return {
            "type": self.type
        }