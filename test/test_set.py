

def test_redis_server(n=1, HOST="127.0.0.1", PORT=6379):
    import redis
    import time
    r = redis.StrictRedis(host=HOST, port=6379, db=0)
    # r = redis.Redis(host=HOST, port=PORT, db=0)
    stime = time.time()
    try:
        for x in range(n):
            r.set('foo_%s' % x, 'bar')
            r.get('foo_%s' % x)
            r.delete('foo_%s' % x)
            r.dump('foo_%s' % x)
    except Exception,e:
        #print e
        pass
    print time.time() - stime


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port

    import threading
    threads = []
    for x in range(100):
        t = threading.Thread(target=test_redis_server, args=(100,))
        threads.append(t)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

