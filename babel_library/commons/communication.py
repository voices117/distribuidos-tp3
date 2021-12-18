from babel_library.commons.socket import Socket

def open(address, port):
    s = Socket()
    s.connect(address, port)

def close(socket):
    socket.close()

def send_request(socket, request, timeout=0):
    socket.set_timeout(timeout)
    socket.send(request)
    res = socket.receive()
    socket.close()

    return res

def send_request_to(address, port, request, timeout=0):
    s = Socket(timeout)
    s.connect(address, port)
    s.send(request)
    res = s.receive()
    s.close()

    return res