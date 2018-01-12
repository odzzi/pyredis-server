

def test_redis_server(n=1, host="127.0.0.1", port=6379):
    import redis
    import time
    r = redis.StrictRedis(host=host, port=port, db=1)
    stime = time.time()
    try:
        for x in range(n):
            r.set('foo_%s' % x, 'bar')
            r.get('foo_%s' % x)
            r.delete('foo_%s' % x)
            r.dump('foo_%s' % x)
            r.keys('foo_%s' % x)
    except Exception,e:
        #print e
        pass
    print time.time() - stime


if __name__ == "__main__":
    import sys
    import threading
    HOST = "127.0.0.1"
    PORT = 6379
    if len(sys.argv) == 2:
        PORT = int(sys.argv[1])
    elif len(sys.argv) == 3:
        HOST = sys.argv[1]
        PORT = sys.argv[2]
    threads = []
    for x in range(1):
        t = threading.Thread(target=test_redis_server, args=(1, HOST, PORT))
        threads.append(t)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

