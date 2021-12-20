"""
This module interfaces with the distributed storage service.

TODO: add integration with the real service once implemented.
"""
from babel_library_client.borges import Borges
from babel_library.commons import constants 
import base64
#import requests

#from typing import Optional


# class Session(requests.Session):

#     def __init__(self, address, port) -> None:
#         self._address = f'http://{address}:{port}'
#         super().__init__()

#     def get(self, uri, **kwargs):
#         return super().get(self._address + uri, **kwargs)

#     def post(self, uri, **kw):
#         return super().post(self._address + uri, **kw)

#     def delete(self, uri, **kw):
#         return super().delete(self._address + uri, **kw)


# _session:Optional[Session] = None
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

def connect(address):
    """Opens a global connection to the service at the given address."""
    global storage_client
    storage_client = Borges()
    # global _session
    # _session = Session(address=address, port=9999)


def set(id:str, key:str, value:bytes):
    """Write or replace the value associated to `key`."""
    payload = bytestoBase64(value)
    storage_client.save(id, key, payload)

    # assert _session
    # r = _session.post(f'/{id}-{key}', data=value)
    # assert r.ok


def read(id:str, key:str):
    """Reads the value associated to the given `key` in the storage."""
    res = storage_client.read(id, key)

    if res["status"] != constants.OK_STATUS:
        return None
    else:
        return base64toBytes(res["message"])

    # assert _session
    # r = _session.get(f'/{id}-{key}')
    # if r.status_code == 404:
    #     return None

    # assert r.ok
    # return r.content


def delete(id:str, key:str):
    """Deletes the key and value from the storage."""
    storage_client.delete(id, key)
    # assert _session
    # r = _session.delete(f'/{id}-{key}')
    # assert r.ok
