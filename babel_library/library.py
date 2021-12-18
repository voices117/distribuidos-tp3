import os
import babel_library.commons.constants as constants
from pathlib import Path
from babel_library.commons.helpers import intTryParse

WORKER_ID = intTryParse(os.environ.get('WORKER_ID')) or 1

class Library:
    def __init__(self):
        pass

    def handle_read(self, request):
        try:
            with open(f'./data_{WORKER_ID}/{request.client}/{request.stream}', "r") as file:
                payload = file.read()
        except Exception as error:
            raise str(error)

        return payload

    def handle_write(self, request):
        try: 
            Path(f'./data_{WORKER_ID}/{request.client}').mkdir(parents=True, exist_ok=True)
        except Exception as error:
            raise str(error)

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
        startpath = f'./data_{WORKER_ID}'

        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            indent = ' ' * 4 * (level)
            print('{}{}/'.format(indent, os.path.basename(root)))
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                print('{}{}'.format(subindent, f))