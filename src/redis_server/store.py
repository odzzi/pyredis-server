import threading
import time


class database:
    DATA = {}
    DATABASES = [{} for x in range(16)]
    TTL = {}
    LOCK = threading.Lock()
    CONFIG = {"databases": "16"}

    @staticmethod
    def select(db_index):
        if database.LOCK.acquire():
            database.DATA = database.DATABASES[int(db_index)]
            database.LOCK.release()

    @staticmethod
    def set(key, value, ext):
        seconds = None
        milliseconds = None
        mode = None
        if ext:
            if "EX" in ext:
                seconds = ext[ext.index("EX") + 1]
            if "PX" in ext:
                milliseconds = ext[ext.index("PX") + 1]
            if "NX" in ext:
                mode = "NX"
            if "XX" in ext:
                mode = "XX"
        if mode == "NX" and (database.get(key) is not None):
            return None
        if mode == "XX" and (database.get(key) is None):
            return None

        if database.LOCK.acquire():
            database.DATA[key] = value
            database.LOCK.release()
        if seconds:
            database.expire(key, seconds)
        if milliseconds:
            database.pexpire(key, milliseconds)

        return "OK"

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
    def get_pttl(key):
        if database.get(key) is None:
            return -2
        ttl = database.TTL.get(key)
        if ttl:
            ttl = ttl - time.time()
            return int(ttl * 1000)
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
    def pexpire(key, ttl):
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA:
                database.TTL[key] = time.time() + float(ttl)/1000
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

    @staticmethod
    def pexpireat(key, ttl_time):
        ttl_time = float(ttl_time) / 1000
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA and time.time() < ttl_time:
                database.TTL[key] = ttl_time
            else:
                ret = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def persist(key):
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA and key in database.TTL:
                del database.TTL[key]
            else:
                ret = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def move(key, db_index):
        ret = 1
        if database.LOCK.acquire():
            if key in database.DATA:
                database.DATABASES[int(db_index)][key] = database.DATA.pop(key)
            else:
                ret = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def randomkey():
        import random
        keys = database.DATA.keys()
        if keys:
            ret = keys[random.randint(0, len(keys))]
            return ret
        else:
            return None

    @staticmethod
    def rename(key, newkey):
        ret = "OK"
        if database.LOCK.acquire():
            if key in database.DATA:
                database.DATA[newkey] = database.DATA.pop(key)
                if key in database.TTL:
                    database.TTL[newkey] = database.TTL.pop(key)
            else:
                ret = "-ERR no such key"
        database.LOCK.release()
        return ret

    @staticmethod
    def renamenx(key, newkey):
        ret = 0
        if database.LOCK.acquire():
            if key in database.DATA and newkey not in database.DATA:
                database.DATA[newkey] = database.DATA.pop(key)
                if key in database.TTL:
                    database.TTL[newkey] = database.TTL.pop(key)
            else:
                ret = 1
        database.LOCK.release()
        return ret

    @staticmethod
    def dump(key):
        ret = database.get(key)
        if ret:
            import pickle
            return pickle.dumps(ret)
        return ret

    @staticmethod
    def restore(key, ttl, serialized_value):
        ret = "OK"
        import pickle
        if database.LOCK.acquire():
            try:
                value = pickle.loads(serialized_value)
                database.DATA[key] = value
                ttl = int(ttl)
                if ttl:
                    database.expire(key, ttl)
            except:
                ret = "-ERR DUMP payload version or checksum are wrong"
        database.LOCK.release()
        return ret

    @staticmethod
    def append(key, value):
        ret = 0
        if database.LOCK.acquire():
            if key in database.DATA:
                database.DATA[key] = database.DATA[key] + value
            else:
                database.DATA[key] = value
            ret = len(database.DATA[key])
        database.LOCK.release()
        return ret

    @staticmethod
    def setbit(key, offset, value):
        ret = 0
        offset = int(offset)
        value = int(value)
        if database.LOCK.acquire():
            if key in database.DATA:
                old = database.DATA[key]
                ret = (old >> offset) & 0x01
                if value == 1:
                    database.DATA[key] = old | (value << offset)
                else:
                    database.DATA[key] = old & (value << offset)
            else:
                database.DATA[key] = value << offset
                ret = value
        database.LOCK.release()
        return ret

    @staticmethod
    def getbit(key, offset):
        ret = 0
        offset = int(offset)
        if database.LOCK.acquire():
            if key in database.DATA:
                old = database.DATA[key]
                ret = (old >> offset) & 0x01
            else:
                database.DATA[key] = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def bitcount(key, start, end):
        ret = 0
        if start:
            start = int(start)
        if end:
            end = int(end)
        if database.LOCK.acquire():
            if key in database.DATA:
                value = database.DATA[key]
                ret = bin(value)[2:][::-1][start:end].count("1")
            else:
                database.DATA[key] = 0
        database.LOCK.release()
        return ret

    @staticmethod
    def bitop(subaction, destkey, keys):
        ret = 0
        subaction = subaction.lower()
        if database.LOCK.acquire():
            values = map(lambda x: database.DATA[x], keys)
            values0 = None
            if values:
                value0 = values[0]
            if subaction == "and":
                for value in values[1:]:
                    value0 &= value
            elif subaction == "or":
                for value in values[1:]:
                    value0 |= value
            elif subaction == "xor":
                for value in values[1:]:
                    value0 ^= value
            elif subaction == "not":
                    value0 = ~(database.DATA[destkey])
            database.DATA[destkey] = value0
            strValue = hex(value0)[2:]
            ret = len(strValue)/2
        database.LOCK.release()
        return ret

    @staticmethod
    def decr(key, amount):
        ret = 0
        if database.LOCK.acquire():
            try:
                value = int(database.DATA.get(key, 0))
                database.DATA[key] = "%s" % (value - int(amount))
                ret = database.DATA[key]
            except :
                ret = "-ERR value is not an integer or out of range"
        database.LOCK.release()
        return ret

    @staticmethod
    def incr(key, amount):
        ret = 0
        if database.LOCK.acquire():
            try:
                value = int(database.DATA.get(key, 0))
                database.DATA[key] = "%s" % (value + int(amount))
                ret = database.DATA[key]
            except :
                ret = "-ERR value is not an integer or out of range"
        database.LOCK.release()
        return ret

    @staticmethod
    def incr_float(key, amount):
        ret = 0
        if database.LOCK.acquire():
            try:
                value = float(database.DATA.get(key, 0))
                database.DATA[key] = "%s" % (value + float(amount))
                ret = database.DATA[key]
            except Exception, e:
                print e
                ret = "-ERR value is not an integer or out of range"
        database.LOCK.release()
        return str(ret)

    @staticmethod
    def getrange(key, start, end):
        start, end = int(start), int(end)
        value = database.DATA.get(key)
        if value:
            return value[start:end]
        return None

    @staticmethod
    def getset(key, value):
        ret = database.DATA.get(key, None)
        if database.LOCK.acquire():
            database.DATA[key] = value
        database.LOCK.release()
        return ret

    @staticmethod
    def mget(keys):
        ret = map(lambda key: database.DATA.get(key, None), keys)
        return ret

    @staticmethod
    def mset(keys, values):
        data = { }
        for key, value in zip(keys, values):
            data[key] = value
        if database.LOCK.acquire():
            database.DATA.update(data)
        database.LOCK.release()
        return ["OK"]

    @staticmethod
    def msetnx(keys, values):
        data = { }
        for key, value in zip(keys, values):
            if database.DATA.get(key) is not None:
                return 0
            data[key] = value
        if database.LOCK.acquire():
            database.DATA.update(data)
        database.LOCK.release()
        return 1


TTL_THREAD_RUNNING = True
def ttl_thread():
    while TTL_THREAD_RUNNING:
        time.sleep(1)
        now = time.time()
        keys = database.TTL.keys()
        keys_to_del = []
        for key in keys:
            if now - database.TTL[key] >= 0:
                del database.TTL[key]
                keys_to_del.append(key)
        database.DEL(keys_to_del)


# initial code
TTL_THREAD = threading.Thread(target=ttl_thread)
# TTL_THREAD.start()
database.DATA = database.DATABASES[0]