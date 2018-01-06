
data = {}

def set(key, value):
    data[key] = value

def get(key):
    return data.get(key, None)