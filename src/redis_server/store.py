import threading


class database:
    DATA = {}
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
        ret = database.DATA.keys()
        return ret


    @staticmethod
    def get_type(key):
        return "string"

    @staticmethod
    def get_ttl(key):
        return -1

    @staticmethod
    def get_config(key):
        return [key, database.CONFIG.get(key, None)]

    @staticmethod
    def set_config(key, value):
        database.CONFIG[key] = value
        return "OK"