import threading
import SocketServer
import operation
import store
import logging

logging.basicConfig(level=logging.DEBUG)


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        s = self.request
        try:
            while True:
                paras = []
                f = s.makefile()
                count = f.readline()
                # resp = operation.encode_para(["-ERR unknown error"])
                if count.startswith("*"):
                    count = int(count[1:].strip())
                    logging.debug(count)

                    for x in range(count):
                        length = f.readline().strip()
                        logging.debug(length)
                        if length.startswith("$"):
                            length = int(length[1:])
                            value = f.read(length)
                            f.read(2)  # skip CR LF
                            logging.debug("%s, %s", length, value)
                            paras.append(value)
                        else:
                            # resp = operation.encode_para(["-ERR parameters error"])
                            pass
                    ret = operation.handle_req(paras)
                    if len(ret) > 1:
                        resp = "*%s\r\n%s" % (len(ret), "".join(ret))
                    else:
                        resp = "".join(ret)

                else:
                    resp = operation.encode_para(["-ERR parameters error"])

                f.write(resp)
                logging.debug("%s => %s" % (str(paras), repr(resp)))
        except Exception, e:
            logging.warn("%s: %s", s.getpeername(), str(e))


class RedisServer:
    def __init__(self, host, port=6379):
        self.HOST = host
        self.PORT = port
        self.server = None

    def start(self, daemon=False, req_queue_size=100):
        class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
            request_queue_size = req_queue_size
            pass

        store.TTL_THREAD_RUNNING = True
        store.TTL_THREAD.start()
        self.server = ThreadedTCPServer((self.HOST, self.PORT), ThreadedTCPRequestHandler)
        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = daemon
        server_thread.start()
        print "pyredis-server listening on %s:%s" % (self.HOST, self.PORT)

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None
            store.TTL_THREAD_RUNNING = False
