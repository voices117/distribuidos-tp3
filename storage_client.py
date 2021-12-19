from os import replace
from babel_library_client.borges import Borges

from service_config import LIBRARIANS
import random

rand = random.randint(0, len(LIBRARIANS))

TIMEOUT = 2 #seconds
client = Borges(TIMEOUT)

#print(client.save(1, 2, "{ id: 1, msg: DONE }"))
print(client.read(1, 2))
#print(client.delete(1,2))