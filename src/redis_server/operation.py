import store

def do_set(paras):
    return paras

def do_get(paras):
    return paras


oper_map = {
    "SET": do_set,
    "GET": do_get,
}

def no_oper(values):
    return ["opertion error."]

def handle_req(paras):
    oper_name = paras[0]
    oper = oper_map.get(oper_name, no_oper)
    return oper(paras)