import threading

data = {}
LOCK = threading.Lock()


def set(key, value):
    if LOCK.acquire():
        data[key] = value
        LOCK.release()


def get(key):
    return data.get(key, None)


def DEL(keys):
    ret = 0
    if LOCK.acquire():
        for key in keys:
            if data.get(key):
                del data[key]
                ret += 1
        LOCK.release()
    return ret