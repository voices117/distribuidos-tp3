import socket
import json

HEADERSIZE = 10
CHUNK_SIZE = 16

class Socket: 
    def __init__(self, timeout = None, sock = None):
        if sock is None:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.settimeout(timeout)
        else:
            self.s = sock
        
    def listen(self, max_queue_size, port):
        self.s.bind((socket.gethostname(), port))
        self.s.listen(max_queue_size)
        

    def attend(self):
        clientsocket, address = self.s.accept()
        return Socket(sock=clientsocket)
            
    def connect(self, host, port):
        self.s.connect((host, port))

    def close(self):
        self.s.close()

    def end(self):
        self.s.close()

    def send(self, data):
        msg = json.dumps(data)
        msg = f"{len(msg):<{HEADERSIZE}}" + msg
        self.s.send(bytes(msg,"utf-8"))

    def receive(self):
        full_msg = ''
        new_msg = True
        end = False
        while end is not True:
            msg = self.s.recv(CHUNK_SIZE)

            if new_msg:
                msglen = int(msg[:HEADERSIZE])
                new_msg = False

            full_msg += msg.decode("utf-8")

            if len(full_msg)-HEADERSIZE == msglen:
                data = json.loads(full_msg[HEADERSIZE:])
                end = True

        return data

    def set_timeout(self, timeout):
        self.s.settimeout(timeout)