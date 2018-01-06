
data = {}

def set(key, value):
    data[key] = value

def get(key):
    return data.get(key, None)

def DEL(keys):
    ret = 0
    for key in keys:
        if data.get(key):
            del data[key]
            ret += 1
    return ret