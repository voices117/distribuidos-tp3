"""
TODO: delete this file once the real services are implemented
"""

import json

from typing import Dict
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse


_data_:Dict[str, str] = {}


class RequestHandler(BaseHTTPRequestHandler):

    def __init__(self, *args, **kw) -> None:
        super().__init__(*args, **kw)

    def log_request(self, *args, **kw):
        # disable logs
        pass

    def do_GET(self):
        url = urlparse(self.path)
        if url.path.count('/') != 1:
            return self._bad_request(msg='invalid key', status=500)

        key = url.path[1:]
        if key not in _data_:
            return self._bad_request(msg='key not found', status=404)

        self.send_response(200)
        self.end_headers()
        self.wfile.write(_data_[key])

    def do_POST(self):
        url = urlparse(self.path)
        if url.path.count('/') != 1:
            return self._bad_request(msg='invalid key', status=500)

        key = url.path[1:]
        content_len = int(self.headers.get('Content-Length'))
        value = self.rfile.read(content_len)

        _data_[key] = value
        self._send_json(data={'status': 'ok'}, status=200)

    def do_DELETE(self):
        url = urlparse(self.path)
        if url.path.count('/') != 1:
            return self._bad_request(msg='invalid key', status=500)

        key = url.path[1:]
        del _data_[key]
        self._send_json(data={'status': 'ok'}, status=200)

    def _bad_request(self, msg:str, status:int = 400):
        return self._send_json(data={'error': msg}, status=status)

    def _send_json(self, data:dict, status:int):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    

def start_storage():
    global server
    server = ThreadingHTTPServer(server_address=('', 9999), RequestHandlerClass=RequestHandler)
    server.serve_forever()


if __name__ == '__main__':
    start_storage()
