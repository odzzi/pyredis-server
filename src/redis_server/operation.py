from store import database
import logging


oper_map = { }


def register_oper(**kwds):
    def decorate(f):
        key = kwds.get("key")
        subkey = kwds.get("subkey")
        if key:
            key = key.upper()
            if subkey:
                subkey = subkey.upper()
                if key not in oper_map:
                    oper_map[key] = {}
                oper_map[key][subkey] = f
            else:
                oper_map[key] = f
        return f

    return decorate


def encode_para(paras):
    return map(lambda x: "$%s\r\n%s\r\n" % (len(x), x) if x else "$-1\r\n", paras)

def no_oper(paras):
    logging.debug("operation not implemented: %s", str(paras))
    return encode_para(["-ERR opertion error."])


def handle_req(paras):
    oper_name = paras[0].upper()
    oper = oper_map.get(oper_name, no_oper)
    if type(oper) is dict:
        if len(paras) > 1:
            suboper_name = paras[1].upper()
            oper = oper.get(suboper_name, no_oper)
        else:
            oper = no_oper
    return oper(paras)


@register_oper(key="SET")
def do_set(paras):
    if len(paras) != 3:
        return encode_para(["-ERR parameters error"])

    action, key, value = paras

    database.set(key=key, value=value)
    return encode_para(["OK"])


@register_oper(key="GET")
def do_get(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras

    return encode_para([database.get(key=key)])


@register_oper(key="DEL")
def do_del(paras):
    if len(paras) < 2:
        return encode_para(["-ERR parameters error"])

    return [":%s\r\n" % database.DEL(paras[1:])]

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
    value = database.get(key)
    if value:
        ret = hex(checksum(value))
    else:
        ret = ["$-1\r\n"]
    return encode_para([ret])


INFO="""# Server
redis_version:2.5.9
redis_git_sha1:473f3090
redis_git_dirty:0
os:Linux 3.3.7-1-ARCH i686
arch_bits:32
multiplexing_api:epoll
gcc_version:4.7.0
process_id:8104
run_id:bc9e20c6f0aac67d0d396ab950940ae4d1479ad1
tcp_port:6379
uptime_in_seconds:7
uptime_in_days:0
lru_clock:1680564

# Clients
connected_clients:1
client_longest_output_list:0
client_biggest_input_buf:0
blocked_clients:0

# Memory
used_memory:439304
used_memory_human:429.01K
used_memory_rss:13897728
used_memory_peak:401776
used_memory_peak_human:392.36K
used_memory_lua:20480
mem_fragmentation_ratio:31.64
mem_allocator:jemalloc-3.0.0

# Persistence
loading:0
rdb_changes_since_last_save:0
rdb_bgsave_in_progress:0
rdb_last_save_time:1338011402
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:-1
rdb_current_bgsave_time_sec:-1
aof_enabled:0
aof_rewrite_in_progress:0
aof_rewrite_scheduled:0
aof_last_rewrite_time_sec:-1
aof_current_rewrite_time_sec:-1

# Stats
total_connections_received:1
total_commands_processed:0
instantaneous_ops_per_sec:0
rejected_connections:0
expired_keys:0
evicted_keys:0
keyspace_hits:0
keyspace_misses:0
pubsub_channels:0
pubsub_patterns:0
latest_fork_usec:0

# Replication
role:master
connected_slaves:0

# CPU
used_cpu_sys:0.03
used_cpu_user:0.01
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Keyspace
"""


@register_oper(key="INFO")
def do_info(paras):
    if len(paras) > 2:
        return encode_para(["-ERR parameters error"])

    #action, key = paras
    return encode_para([INFO.replace("\n", "\r\n")])


@register_oper(key="CONFIG")
def do_config(paras):
    if len(paras) < 2:
        return encode_para(["-ERR parameters error"])

    action, oper = paras[:2]
    ret = []
    if oper.lower() == "get":
        if len(paras) != 3:
            return encode_para(["-ERR parameters error"])
        ret = database.get_config(paras[2])

    return encode_para(ret)


@register_oper(key="KEYS")
def do_keys(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras
    ret = database.keys(key)
    if ret:
        return encode_para(ret)
    else:
        return ["*0\r\n"]


@register_oper(key="TYPE")
def do_type(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras
    ret = database.get_type(key)
    if ret:
        return encode_para([ret])
    else:
        return ["$-1\r\n"]


@register_oper(key="TTL")
def do_ttl(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras
    ret = database.get_ttl(key)
    if ret:
        return [":%s\r\n" % ret]
    else:
        return ["$-2\r\n"]


@register_oper(key="OBJECT", subkey="REFCOUNT")
def do_object_refcount(paras):
    if len(paras) != 3:
        return encode_para(["-ERR parameters error"])

    action, subaction, key = paras
    return [":1\r\n"]


@register_oper(key="OBJECT", subkey="IDLETIME")
def do_object_ideltime(paras):
    if len(paras) != 3:
        return encode_para(["-ERR parameters error"])

    action, subaction, key = paras
    return [":100\r\n"]


@register_oper(key="OBJECT", subkey="ENCODING")
def do_object_ideltime(paras):
    if len(paras) != 3:
        return encode_para(["-ERR parameters error"])

    action, subaction, key = paras
    return encode_para(["raw"])


@register_oper(key="EXISTS")
def do_exists(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])

    action, key = paras
    ret = database.get(key)
    if ret:
        return [":1\r\n"]
    else:
        return [":0\r\n"]


@register_oper(key="SELECT")
def do_select(paras):
    if len(paras) != 2:
        return encode_para(["-ERR parameters error"])
    logging.debug("SELECT %s", str(paras))
    return ["$2\r\nOK\r\n"]

