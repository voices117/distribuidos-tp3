
import os
import random
from babel_library.library import Library
from commons.socket import Socket
import time

MAX_QUEUE_SIZE = os.environ.get('MAX_QUEUE_SIZE') or 5
PORT = os.environ.get('PORT') or random.randint(5000, 6000)

class Librarian:
    def __init__(self):
        self.library = Library()
        self.sock = Socket()
        client = self.sock.listen(MAX_QUEUE_SIZE, PORT)
        print(f"Librarian listening on port {PORT}.")
        
        while True:
            client = self.sock.attend()
            req = client.receive()
            res = self.dispatch(req)
            client.send(res)
            client.close()

    def dispatch(self, request):
        return self.library.handle(request)