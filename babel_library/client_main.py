from babel_library_client.borges import Borges

TIMEOUT = 2 #seconds
PORT = 5002 #TODO: Resolve
HOST = '173.105.125.2' #TODO: Resolve

client = Borges(HOST, PORT, TIMEOUT)

#client.save(1, 1, "{ id: 1, msg: DONE }")
print(client.read(1, 1))