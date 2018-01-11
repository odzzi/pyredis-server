import store
import logging


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
    logging.debug("operation not implemented: %s", str(paras))
    return encode_para(["-ERR opertion error."])


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

import array
def checksum(data):
    if len(data) % 2:
        data += b'\x00'
    s = sum(array.array('H',data))
    s = (s & 0xffff) + (s >> 16)
    s += (s >> 16)
    return (~s & 0xffff)


@register_oper(key="DUMP")
def do_dump(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras
    value = store.get(key)
    if value:
        ret = hex(checksum(value))
    else:
        ret = "$-1"
    return encode_para([ret])


@register_oper(key="SELECT")
def do_select(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])
    logging.debug("SELECT %s", str(paras))
    return ["$2\r\nOK\r\n"]

