import store

oper_map = { }


def register_oper(**kwds):
    def decorate(f):
        key = kwds.get("key")
        if key:
            oper_map[key] = f
        return f

    return decorate


def encode_para(paras):
    return map(lambda x: "$%s\r\n%s\r\n" % (len(x), x) if x else "$-1\r\n", paras)

def no_oper(paras):
    print paras
    return ["-ERR opertion error."]


def handle_req(paras):
    oper_name = paras[0]
    oper = oper_map.get(oper_name, no_oper)
    return oper(paras)


@register_oper(key="SET")
def do_set(paras):
    if len(paras) != 3:
        return encode_para(["-ERR parameters error"])

    action, key, value = paras

    store.set(key=key, value=value)
    return encode_para(["OK"])


@register_oper(key="GET")
def do_get(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras

    return encode_para([store.get(key=key)])


@register_oper(key="DEL")
def do_del(paras):
    if len(paras) < 2:
        return encode_para(["-ERR parameters error"])

    return [":%s\r\n" % store.DEL(paras[1:])]
