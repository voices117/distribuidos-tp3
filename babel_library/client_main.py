from babel_library_client.borges import Borges
import socket

TIMEOUT = 1 #seconds
PORT = 5143 #TODO: Resolve
HOST = socket.gethostname() #TODO: Resolve

client = Borges(HOST, PORT, TIMEOUT)

client.save(1, 1, "asdqew")

#print(client.read(2, 2))