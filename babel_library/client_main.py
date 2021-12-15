from os import replace
from babel_library_client.borges import Borges
import socket

TIMEOUT = 2 #seconds
PORT = 5847 #TODO: Resolve
HOST = socket.gethostname()#"173.105.125.4"#

client = Borges(HOST, PORT, TIMEOUT)

client.save(1, 1, "{ id: 1, msg: DONE }", replace=True)
#print(client.read(1, 1))
#client.delete(1,1)