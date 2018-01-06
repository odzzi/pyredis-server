import store

oper_map = { }


def register_oper(**kwds):
    def decorate(f):
        key = kwds.get("key")
        if key:
            oper_map[key] = f
        return f

    return decorate


@register_oper(key="SET")
def do_set(paras):
    if len(paras) != 3:
        return ["parameters error"]

    action, key, value = paras

    store.set(key=key, value=value)
    return ["0"]


@register_oper(key="GET")
def do_get(paras):
    if len(paras) != 2:
        return ["parameters error"]

    action, key = paras

    return [store.get(key=key)]



def no_oper(values):
    return ["opertion error."]

def handle_req(paras):
    oper_name = paras[0]
    oper = oper_map.get(oper_name, no_oper)
    return oper(paras)