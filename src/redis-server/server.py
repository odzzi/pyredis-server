import socket
import threading
import SocketServer
import socket
import operation

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        s = self.request
        f = s.makefile()
        count = f.readline()
        if count.startswith("*"):
            count = int(count[1:].strip())
            print count
            paras = []
            for x in range(count):
                length = f.readline()
                if length.startswith("$"):
                    length = int(length[1:].strip())
                    value = f.read(length)
                    f.read(2)  # skip CR LF
                    print length, value
                    paras.append(value)
                else:
                    print length
            f.write("\r\n".join(operation.handle_req(paras)))

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

def client(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    try:
        sock.sendall(message)
        response = sock.recv(1024)
        print "Received: {}".format(response)
    finally:
        sock.close()

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "localhost", 0

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name

    req = "\r\n".join(["*3", "$3","HSET","$5","my\x17ey","$7", "myvalue"])
    print req
    client(ip, port, "%s\r\n" % req)

    server.shutdown()
    server.server_close()