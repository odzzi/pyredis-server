import sys

from redis_server import server


def showUsage():
    print "usage:"
    print "","python main.py PORT"
    print "","python main.py IP PORT"

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 6379
    if len(sys.argv) == 2:
        try:
            PORT = int(sys.argv[1])
        except:
            showUsage()
            exit(1)

    if len(sys.argv) == 3:
        try:
            HOST = sys.argv[1]
            PORT = int(sys.argv[2])
        except:
            showUsage()
            exit(1)

    s = server.redis_server(HOST=HOST, PORT=PORT)
    s.start()