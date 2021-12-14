import os
import commons.constants as constants
import json
from pathlib import Path
from commons.helpers import intTryParse

WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

class Library:
    def __init__(self):
        pass

    def handle_read(self, request):
        try:
            with open(f'./data_{WORKER_ID}/{request.client}/{request.stream}', "r") as file:
                payload = file.read()
        except Exception as error:
            print(error)
            raise { "status": constants.ERROR_STATUS, "message": "Error reading."}

        return payload

    def handle_write(self, request):
        try: 
            Path(f'./data_{WORKER_ID}/{request.client}').mkdir(parents=True, exist_ok=True)
        except Exception as error:
            print(error)
            raise { "status": constants.ERROR_STATUS, "message": "Error writing."}

        mode = 'a'
        if request.replace:
            mode = 'w'

        with open(f'./data_{WORKER_ID}/{request.client}/{request.stream}', mode) as file:
            json.dump(request.payload, file)
            file.write('\n')

        return { "status": constants.OK_STATUS }

    def handle_delete(self, request):
        try:
            path = f'./data_{WORKER_ID}/{request.client}/{request.stream}'
            if os.path.exists(path):
                os.remove(path)
        except Exception as error:
            print(error)
            raise { "status": constants.ERROR_STATUS, "message": "Error removing."}
        
        return { "status": constants.OK_STATUS }