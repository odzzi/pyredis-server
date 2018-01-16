import threading
import time


class database:
    DATA = {}
    TTL = {}
    LOCK = threading.Lock()
    CONFIG = {"databases": "16"}

    @staticmethod
    def set(key, value):
        if database.LOCK.acquire():
            database.DATA[key] = value
            database.LOCK.release()

    @staticmethod
    def get(key):
        return database.DATA.get(key, None)

    @staticmethod
    def DEL(keys):
        ret = 0
        if database.LOCK.acquire():
            for key in keys:
                if database.DATA.get(key):
                    del database.DATA[key]
                    ret += 1
        database.LOCK.release()
        return ret

    @staticmethod
    def keys(key):
        import re
        patten = re.compile(key.replace("*", r"[\w]*").replace("?", "[\w]"))
        ret = filter(lambda x: patten.match(x), database.DATA.keys())
        return ret

    @staticmethod
    def get_type(key):
        return "string"

    @staticmethod
    def get_config(key):
        return [key, database.CONFIG.get(key, None)]

    @staticmethod
    def set_config(key, value):
        database.CONFIG[key] = value
        return "OK"

    @staticmethod
    def get_ttl(key):
        if database.get(key) is None:
            return -2
        ttl = database.TTL.get(key)
        if ttl:
            ttl = ttl - time.time()
            return int(ttl)
        return -1

    @staticmethod
    def expire(key, ttl):
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA:
                database.TTL[key] = time.time() + int(ttl)
            else:
                ret = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def expireat(key, ttl_time):
        ttl_time = float(ttl_time)
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA and time.time() < ttl_time:
                database.TTL[key] = ttl_time
            else:
                ret = 0
        database.LOCK.release()
        return ret


def ttl_thread():
    while True:
        time.sleep(1)
        now = time.time()
        keys = database.TTL.keys()
        keys_to_del = []
        for key in keys:
            if now - database.TTL[key] >= 0:
                del database.TTL[key]
                keys_to_del.append(key)
        database.DEL(keys_to_del)


TTL_THREAD = threading.Thread(target=ttl_thread)
TTL_THREAD.start()