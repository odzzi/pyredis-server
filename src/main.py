import socket
from redis_server import server


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 6379

    s = server.redis_server(HOST=HOST, PORT=PORT)
    s.start()