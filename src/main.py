import socket
from redis_server import server


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
    HOST, PORT = "localhost", 6379

    s = server.redis_server(HOST=HOST, PORT=PORT)
    s.start()
    req = "\r\n".join(["*3", "$3","SET","$5","mykey","$7", "myvalue"])
    client(HOST, PORT, "%s\r\n" % req)
    req = "\r\n".join(["*2", "$3", "GET", "$5", "mykey"])
    client(HOST, PORT, "%s\r\n" % req)

    s.stop()

