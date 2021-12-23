"""
This module contains logic for the nodes in the system to respond to liveness probes
from the service that controls nodes.

To start the server just call `start_server_in_new_thread` and pass the number of
port where the service should listen.
"""

import json
import logging
import threading

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        """Handles GET requests. Only accepts /status requests and responds with the
        current node status."""

        url = urlparse(self.path)
        if url.path != '/status':
            return self._bad_request(msg='only /status allowed', status=404)

        data = {'status': 'ok'}
        self._send_json(data=data, status=200)

    def log_message(self, *args, **kw):
        pass

    def _bad_request(self, msg:str, status:int = 400):
        return self._send_json(data={'error': msg}, status=status)

    def _send_json(self, data:dict, status:int):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))


def start_server(listen_port:int):
    """Creates and starts a server that responds to HTTP liveness probes.
    This function will block until the server is closed. Use
    `start_server_in_new_thread` to avoid blocking the main thread."""

    server_address = ('', listen_port)
    httpd = ThreadingHTTPServer(server_address, RequestHandler)
    httpd.serve_forever()


def start_server_in_new_thread(listen_port:int = 80) -> threading.Thread:
    """Creates a new thread that runs the web server that responds to HTTP
    liveness probes."""

    logging.info(f'starting liveness server in new thread in port {listen_port}')

    t = threading.Thread(
        target=lambda: start_server(listen_port=listen_port),
        daemon=True
    )
    t.start()
    return t


if __name__ == '__main__':
    t = start_server_in_new_thread(8888)
    t.join()
