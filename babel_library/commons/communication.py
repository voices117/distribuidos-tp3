from babel_library.commons.socket import Socket

def send_request_to(address, port, request, timeout=0):
    s = Socket(timeout)
    s.connect(address, port)
    s.send(request)
    res = s.receive()
    s.close()

    return res