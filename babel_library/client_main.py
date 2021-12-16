from os import replace
from babel_library_client.borges import Borges
import socket

TIMEOUT = 2 #seconds
PORT = 5000 #TODO: Resolve
HOST = "173.105.125.3"#socket.gethostname()#

client = Borges(HOST, PORT, TIMEOUT)

#client.save(1, 1, "{ id: 1, msg: DONE }", replace=False)
#print(client.read(1, 1))
client.delete(1,1)