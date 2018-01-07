import socket
from redis_server import server


def client(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    try:
        sock.sendall(message)
        response = sock.recv(1024)
        print "Received: {}".format(repr(response))
    finally:
        sock.close()

def test_redis_server(n):
    import redis
    import time
    r = redis.StrictRedis(host=HOST, port=6379, db=0)
    # r = redis.Redis(host=HOST, port=PORT, db=0)
    stime = time.time()
    for x in range(n):
        r.set('foo_%s' % x, 'bar')
        r.get('foo_%s' % x)
        r.delete('foo_%s' % x)
        r.get('foo_%s' % x)
    print time.time() - stime


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 6379

    s = server.redis_server(HOST=HOST, PORT=PORT)
    s.start()

    # req = "\r\n".join(["*3", "$3","SET","$5","mykey","$7", "myvalue"])
    # client(HOST, PORT, "%s\r\n" % req)
    # req = "\r\n".join(["*2", "$3", "GET", "$5", "mykey"])
    # client(HOST, PORT, "%s\r\n" % req)
    # req = "\r\n".join(["*2", "$3", "DEL", "$5", "mykey"])
    # client(HOST, PORT, "%s\r\n" % req)
    # req = "\r\n".join(["*2", "$3", "GET", "$5", "mykey"])
    # client(HOST, PORT, "%s\r\n" % req)
    # req = "\r\n".join(["*2", "$3", "GEH", "$5", "mykey"])
    # client(HOST, PORT, "%s\r\n" % req)
    import threading
    threads = []
    for x in range(10):
        t = threading.Thread(target=test_redis_server, args=(100,))
        threads.append(t)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    s.stop()