import json
import logging
import requests
from threading import Lock, Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import time
import cgi
from queue import Queue

ELECTION_SIGNAL = -1
UNKNOWN_LIDER_ID = -1

class Bully(Thread):
    def __init__(self, config) -> None:
        self.Initilize = False
        self.Id = config['id']
        self.Nodes = config['nodes']
        self.Highers = self._getHighers()
        self.coordinatorId = UNKNOWN_LIDER_ID
        self.timeout = config['timeout']
        self.coordinator = False
        self.addr = config['addr']
        self.port = int(config['port'])
        self.server = webServer(self.addr, self.port)
        self.lock = Lock()
        self.running = True
        
        logging.basicConfig(level=logging.INFO)
        self._printConfig()
        self.server.start()
        Thread.__init__(self)
    
    def _printConfig(self):
        logging.info(f'Nodo: {self.Id}, Timeout: {self.timeout}, WebServer: {self.addr}:{self.port}')

    def _getHighers(self):
        highers = {}
        for id, addr in self.Nodes.items():
            if int(id) > self.Id:
                highers[int(id)] = addr
        return highers

    def _getSmallers(self):
        highers = {}
        for id, addr in self.Nodes.items():
            if int(id) < self.Id:
                highers[int(id)] = addr
        return highers    

    def close(self):
        self.server.closeServer()
        self.server.join()
        self.running = False

    def coordinatorIsAlive(self):
        if self.coordinatorId == UNKNOWN_LIDER_ID:
            #waiting for new coordinator coordinator message
            return True
        try:
            response = requests.get(url=self.Highers[self.coordinatorId] + '/status', timeout=self.timeout)
            if response.status_code == requests.codes.ok:
                return True
            return False
        except:
            return False

    def _waitAckSignal(self):
        timer = 0
        message = self.server.getAck()
        while timer != self.timeout:
            if message:
                if message != self.Id:
                    logging.info(f'[nodo: {self.Id}] an error ocurred, id receibed {message}')
                    return False
                return True
            time.sleep(1)
            timer += 1
            message = self.server.getAck()
        return True

    def _startElection(self):
        logging.info(f'[nodo: {self.Id}] Starting new election')
        self.lock.acquire()
        higherId = -1
        for id, addr in self.Highers.items():
            try:
                r = requests.post(url=addr+'/election', data={'id': ELECTION_SIGNAL})
                if r.status_code == requests.codes.ok:
                    higherId = id
            except:
                continue
        if higherId == -1:
            #I am the new coordinator
            logging.info(f'[nodo: {self.Id}] maybe I am the new coordinator')
            self._postNewLider()
            if self._waitAckSignal():
                self.coordinatorId = self.Id
                self.coordinator = True
                logging.info(f'[nodo: {self.Id}] I am the new coordinator')
            else:
                self.coordinatorId = UNKNOWN_LIDER_ID
                logging.info(f'[nodo: {self.Id}] I am NOT the new coordinator')
        else:
            self.coordinatorId = UNKNOWN_LIDER_ID
        self.lock.release()
        logging.info(f'[nodo: {self.Id}] Ending election')

    def amICoordinator(self):
        with self.lock:
            return self.coordinator

    def _postNewLider(self):
        for _, addr in self._getSmallers().items():
            try:
                requests.post(url=addr+'/coordinator', json={'id': self.Id})
            except:
                logging.info(f'[nodo: {self.Id}] cant establish comunication with {addr}')

    def run(self):
        i = 0
        while self.running:
            if not self.Initilize:
                self._startElection()
                self.Initilize = True
            else:
                if not self.server.emptyInbox():
                    logging.info(f'[nodo: {self.Id}] a message has arrived')
                    message = self.server.getMessage()
                    if  message == ELECTION_SIGNAL:
                        self._startElection()
                    else:
                        self.lock.acquire()
                        self.coordinatorId = message
                        self.coordinator = False
                        if self.coordinator:
                            try:
                                coordinatorAddr = self.Nodes[self.coordinatorId]
                                requests.post(url= coordinatorAddr+'/ack', data={'id': self.coordinatorId})
                            except:
                                self.lock.release()
                                continue 
                        self.lock.release()
                elif self.coordinator:
                    logging.info(f'[nodo: {self.Id}] i am the leader')
                    time.sleep(2)
                    if i%5 == 0:
                        self._postNewLider()
                    i+=1
                    continue
                elif self.coordinatorIsAlive():
                    logging.info(f'[nodo: {self.Id}] coordinator {self.coordinatorId} alive')
                    time.sleep(2.0)
                    continue
                else:
                    logging.info(f'[nodo: {self.Id}] the leader has died')
                    self._startElection()
        
        logging.info(f'[nodo: {self.Id}] closing Bully Algorith')         

class webServer(Thread):    
    def __init__(self, serverAddress, port):
        self.serverAddress = serverAddress
        self.port = port
        self.server = None
        self.messageOutput = Queue()
        self.ackOutput = Queue()
        Thread.__init__(self)

    def _empty(self, queue):
        return queue.empty()

    def _get(self, queue):
        if queue.empty():
            return None
        value = queue.get()
        queue.task_done()    
        return value

    def emptyInbox(self):
        return self._empty(self.messageOutput)

    def emptyAck(self):
        return self._empty(self.ackOutput)

    def getAck(self):
        return self._get(self.ackOutput)

    def getMessage(self):
        return self._get(self.messageOutput)

    def closeServer(self):
        self.server.shutdown()
        logging.info(f'{time.asctime()} server Stops - {self.serverAddress} {self.port}')    

    def run(self):
        server_class = HTTPServer
        self.server = server_class((self.serverAddress, self.port), make_handler(self.messageOutput, self.ackOutput))
        logging.info(f'{time.asctime()} server starts - {self.serverAddress} {self.port}')
        self.server.serve_forever()
        logging.info("exiting from run server")

def make_handler(messageOutput, ackOutput):

    class MyHandler(BaseHTTPRequestHandler):
        def do_HEAD(s):
            s.send_response(200)
            s.send_header("Content-type", "text/html")
            s.end_headers()

        def do_GET(s):
            """Respond to a GET request."""
            if '/status' in s.path:
                #logging.info(f'[HTTP server] a status message arrived')
                s.send_response(200)
                s.send_header("Content-type", "application/json")
                s.end_headers()
                s.wfile.write(json.dumps({}).encode())

        def do_POST(s):
            """Respond to a POST request."""
            if '/election' in s.path:
                messageOutput.put(ELECTION_SIGNAL)
                logging.info("[HTTP server] an election message arrived")
                s.send_response(200)
                s.send_header("Content-type", "application/json")
                s.end_headers()
                s.wfile.write(json.dumps({}).encode())

            if '/coordinator' in s.path:
                logging.info("[HTTP server] a coordinator message arrived")
                ctype, pdict = cgi.parse_header(s.headers.get_content_type())
                if ctype == 'application/json':
                    length = int(s.headers.get('content-length'))
                    message = json.loads(s.rfile.read(length))
                    logging.info(f'recibi: {message}')
                    messageOutput.put(message['id'])
                    s.send_response(200)
                    s.send_header("Content-type", "application/json")
                    s.end_headers()
                    s.wfile.write(json.dumps(message).encode())

            if '/ack' in s.path:
                logging.info("[HTTP server] an ack message arrived")
                ctype, pdict = cgi.parse_header(s.headers.get_content_type())
                if ctype == 'application/json':
                    length = int(s.headers.get('content-length'))
                    message = json.loads(s.rfile.read(length))
                    logging.info(f'recibi: {message}')
                    ackOutput.put(message['id'])
                    s.send_response(200)
                    s.send_header("Content-type", "application/json")
                    s.end_headers()
                    s.wfile.write(json.dumps(message).encode())

    return MyHandler