from os import replace
from babel_library_client.borges import Borges

TIMEOUT = 3 #seconds
client = Borges(TIMEOUT)

#print(client.save(1, 2, "{ id: 1, msg: DONE }"))
#print(client.read(1, 2))
#print(client.delete(1,2))

#print(client.try_lock(1,2))
#print(client.unlock(1,2))