from redis_server import server

def test_redis_server(n=1, host="127.0.0.1", port=6379):
    import redis
    import time

    s = server.RedisServer(host=host, port=port)
    s.start()
    r = redis.StrictRedis(host=host, port=port, db=1)
    stime = time.time()
    try:
        for x in range(n):
            print r.set('foo_%s' % x, 'bar')
            print r.get('foo_%s' % x)
            # print r.expire('foo_%s' % x, 123)
            # print r.pexpireat('foo_%s' % x, int(time.time()*1000 + 123))
            print r.pttl('foo_%s' % x)
            dumpstr = r.dump('foo_%s' % x)
            print repr(dumpstr)
            print r.restore("restored_%s" % x, 0, dumpstr)
            print r.get("restored_%s" % x)
            print r.persist('foo_%s' % x)
        print r.keys("foo_1*")
        # r = redis.StrictRedis(host=host, port=port, db=2)
        print r.keys("foo_1*")
        print r.randomkey()
        print r.rename("nokey", "key")
        print r.rename("foo_1", "newkey")
        print r.keys("*")
        print r.renamenx("newkey", "foo_1")
        print r.keys("*")
        print r.renamenx("foo_1", "foo_11")
        print r.keys("*")
    except Exception,e:
        print e
        pass
    print time.time() - stime
    s.stop()
