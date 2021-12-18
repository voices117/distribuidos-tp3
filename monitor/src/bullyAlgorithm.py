import json
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
        self.server = webServer(config['addr'], int(config['port']))
        self.lock = Lock()
        self.running = True
        
        self._printConfig()
        self.server.start()
        Thread.__init__(self)
    
    def _printConfig(self):
        print(f'Nodo: {self.Id}, coodinator: {self.coordinatorId}, Timeout: {self.timeout}, Highers: {self.Highers}')

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
        print("here 3")
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

    def _startElection(self):
        print(f'[nodo: {self.Id}] Starting new election')
        self.lock.acquire()
        higherId = -1
        for id, addr in self.Highers.items():
            print(f'me voy a comunicar con {addr}')
            try:
                r = requests.post(url=addr+'/election', data={'id': ELECTION_SIGNAL})
                print(f'estado del request a {addr}: {r.status_code}')
                if r.status_code == requests.codes.ok:
                    higherId = id
            except:
                continue
        if higherId == -1:
            #I am the new coordinator
            print(f'[nodo: {self.Id}] I am the new coordinator')
            self._postNewLider()
            self.coordinatorId = self.Id
            self.coordinator = True
        else:
            self.coordinatorId = UNKNOWN_LIDER_ID
        self.lock.release()    
        print(f'[nodo: {self.Id}] Ending election')

    def amICoordinator(self):
        with self.lock:
            return self.coordinator

    def _postNewLider(self):
        for _, addr in self._getSmallers().items():
            try:
                requests.post(url=addr+'/coordinator', json={'id': self.Id})
            except:
                print(f'cant establish comunication with {addr}')

    def run(self):
        i = 0
        while self.running:
            if not self.Initilize:
                self._startElection()
                self.Initilize = True
            else:
                if not self.server.emptyInbox():
                    print("here 1")
                    message = self.server.getMessage()
                    if  message == ELECTION_SIGNAL:
                        self._startElection()
                    else:
                        self.lock.acquire()
                        self.coordinatorId = message
                        self.coordinator = False
                        self.lock.release()
                elif self.coordinator:
                    print("here 2")
                    time.sleep(2)
                    if i%5 == 0:
                        self._postNewLider()
                    i+=1
                    continue
                elif self.coordinatorIsAlive():
                    print(f'[nodo: {self.Id}] Coordinator {self.coordinatorId} alive')
                    time.sleep(2.0)
                    continue
                else:
                    print("here 4")
                    self._startElection()
        
        print("Closing Bully Algorith")         

class webServer(Thread):    
    def __init__(self, serverAddress, port):
        self.serverAddress = serverAddress
        self.port = port
        self.server = None
        self.output = Queue()
        Thread.__init__(self)

    def emptyInbox(self):
        return self.output.empty()

    def getMessage(self):
        if self.output.empty():
            return None
        value = self.output.get()
        self.output.task_done()    
        return value

    def closeServer(self):
        self.server.shutdown()
        print(time.asctime(), "Server Stops - %s:%s" % (self.serverAddress, self.port))    

    def run(self):
        server_class = HTTPServer
        self.server = server_class((self.serverAddress, self.port), make_handler(self.output))
        print(time.asctime(), "Server Starts - %s:%s" % (self.serverAddress, self.port))
        self.server.serve_forever()
        print("exiting from run server")

def make_handler(output):

    class MyHandler(BaseHTTPRequestHandler):
        def do_HEAD(s):
            s.send_response(200)
            s.send_header("Content-type", "text/html")
            s.end_headers()

        def do_GET(s):
            """Respond to a GET request."""
            print(s.path)
            if '/status' in s.path:
                s.send_response(200)
                s.send_header("Content-type", "application/json")
                s.end_headers()
                s.wfile.write(json.dumps({}).encode())

        def do_POST(s):
            if '/election' in s.path:
                output.put(ELECTION_SIGNAL)
                print("me llego un mensaje election")
                s.send_response(200)
                s.send_header("Content-type", "application/json")
                s.end_headers()
                s.wfile.write(json.dumps({}).encode())

            if '/coordinator' in s.path:
                print("me llego un mensaje coordinator")
                ctype, pdict = cgi.parse_header(s.headers.get_content_type())
                if ctype == 'application/json':
                    length = int(s.headers.get('content-length'))
                    message = json.loads(s.rfile.read(length))
                    print('Recibi ', message)
                    output.put(message['id'])
                    s.send_response(200)
                    s.send_header("Content-type", "application/json")
                    s.end_headers()
                    s.wfile.write(json.dumps(message).encode())
    return MyHandler