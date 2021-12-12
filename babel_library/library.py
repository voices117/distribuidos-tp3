import os
import commons.constants as constants
import json
from pathlib import Path
from commons.helpers import intTryParse

WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

class Library:
    def __init__(self):
        pass

    def handle(self, request):
        if request["type"] == constants.READ_REQUEST:
            return self.handle_read(request)
        elif request["type"] == constants.WRITE_REQUEST:
            return self.handle_write(request)

    def handle_read(self, request):
        client = request["client"]
        stream = request["stream"]
        
        try:
            with open(f'./data_{WORKER_ID}/{client}/{stream}', "r") as file:
                payload = file.read()
        except Exception as error:
            raise { "status": constants.ERROR_STATUS, "message": "Error reading."}

        return { "status": constants.OK_STATUS, "content": payload }

    def handle_write(self, request):
        client = request["client"]
        stream = request["stream"]

        try: 
            Path(f'./data_{WORKER_ID}/{client}').mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise { "status": constants.ERROR_STATUS, "message": "Error writing."}

        with open(f'./data_{WORKER_ID}/{client}/{stream}', "a") as file:
            json.dump(request["payload"], file)
            file.write('\n')

        return { "status": constants.OK_STATUS }