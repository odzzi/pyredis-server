import threading
import SocketServer
import operation


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        s = self.request
        try:
            while True:
                paras = []
                f = s.makefile()
                count = f.readline()
                resp = operation.encode_para(["-ERR unknown error"])
                if count.startswith("*"):
                    count = int(count[1:].strip())
                    #print count

                    for x in range(count):
                        length = f.readline()
                        #print length
                        if length.startswith("$"):
                            length = int(length[1:].strip())
                            value = f.read(length)
                            f.read(2)  # skip CR LF
                            #print length, value
                            paras.append(value)
                        else:
                            resp = operation.encode_para(["-ERR parameters error"])
                    ret = operation.handle_req(paras)
                    if len(ret) > 1:
                        resp = "*%s\r\n%s"%(len(ret), "".join(ret))
                    else:
                        resp = "".join(ret)
                    #print repr(resp)

                else:
                    resp = operation.encode_para(["-ERR parameters error"])

                f.write(resp)
                # print "%s end\n" % (str(paras)),
        except Exception, e:
            print e


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    request_queue_size = 100
    pass


class redis_server:
    def __init__(self, HOST, PORT=6379):
        self.HOST = HOST
        self.PORT = PORT
        self.server = None

    def start(self, daemon=False):
        self.server = ThreadedTCPServer((self.HOST, self.PORT), ThreadedTCPRequestHandler)
        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = daemon
        server_thread.start()
        print "pyredis-server listening on %s:%s"% (self.HOST, self.PORT)

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None