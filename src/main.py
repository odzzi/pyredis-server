import sys

from redis_server import server


def show_usage():
    print "usage:"
    print "", "python main.py PORT"
    print "", "python main.py IP PORT"

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 6379
    if len(sys.argv) == 2:
        try:
            PORT = int(sys.argv[1])
        except ValueError, e:
            print e
            show_usage()
            exit(1)

    if len(sys.argv) == 3:
        try:
            HOST = sys.argv[1]
            PORT = int(sys.argv[2])
        except ValueError, e:
            print e
            show_usage()
            exit(1)

    s = server.RedisServer(host=HOST, port=PORT)
    s.start()
