from os import replace
from babel_library_client.borges import Borges
import socket
from service_config import LIBRARIANS
import random

rand = random.randint(0, len(LIBRARIANS))

TIMEOUT = 2 #seconds
PORT = 5000 #LIBRARIANS[rand]["port"]
HOST = "173.150.125.2" #LIBRARIANS[rand]["name"]

client = Borges(HOST, PORT, TIMEOUT)

client.save(1, 1, "{ id: 1, msg: DONE }", replace=False)
#print(client.read(1, 1))
#client.delete(1,1)