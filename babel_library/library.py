import os
import babel_library.commons.constants as constants
from pathlib import Path
from babel_library.commons.helpers import intTryParse
import json

WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

class Library:
    def __init__(self):
        pass

    def handle_read(self, request):
        with open(f'./data_{WORKER_ID}/{request.client}/{request.stream}', "r") as file:
            payload = file.read()

        return payload

    def handle_write(self, request):
        try: 
            Path(f'./data_{WORKER_ID}/{request.client}').mkdir(parents=True, exist_ok=True)
        except Exception as error:
            raise Exception(str(error)) 

        mode = 'a'
        if request.replace:
            mode = 'w'

        with open(f'./data_{WORKER_ID}/{request.client}/{request.stream}', mode) as file:
            file.write(request.payload)
            file.write('\n')


    def handle_delete(self, request):
        try:
            path = f'./data_{WORKER_ID}/{request.client}/{request.stream}'
            if os.path.exists(path):
                os.remove(path)
        except Exception as error:
            raise str(error)


    def list_files(self):
        responses = []

        client_directories = os.scandir(f'./data_{WORKER_ID}/')
        for cd in client_directories:
            stream_files = os.scandir(f'./data_{WORKER_ID}/{cd.name}')
            for sd in stream_files:
                responses.append({ "client": cd.name, "stream": sd.name })

        return json.dumps(responses)