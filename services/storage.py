"""
This module interfaces with the distributed storage service.
"""
from babel_library_client.borges import Borges
from babel_library.commons import constants 
import base64

storage_client = None

def bytestoBase64(input):
    try:
        return base64.b64encode(input).decode('utf-8')
    except Exception as err:
        print(err, input)

def base64toBytes(input):
    try:
        return base64.b64decode(input)
    except Exception as err:
        print(err, input)

def connect():
    """Opens a global connection to the service."""
    global storage_client
    storage_client = Borges()


def set(id:str, key:str, value:bytes):
    """Write or replace the value associated to `key`."""
    payload = bytestoBase64(value)
    storage_client.save(id, key, payload)


def read(id:str, key:str):
    """Reads the value associated to the given `key` in the storage."""
    res = storage_client.read(id, key)

    if res["status"] != constants.OK_STATUS:
        return None
    else:
        return base64toBytes(res["message"])


def delete(id:str, key:str):
    """Deletes the key and value from the storage."""
    storage_client.delete(id, key)
