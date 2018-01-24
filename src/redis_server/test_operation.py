import operation


def test_key_set():
    ret = operation.handle_req(["SET", "KEY", "123"])
    assert(ret == ["$2\r\nOK\r\n"])


def test_key_get():
    operation.handle_req(["SET", "KEY", "123"])
    ret = operation.handle_req(["GET", "KEY"])
    assert(ret == ["$3\r\n123\r\n"])

    operation.handle_req(["SET", "KEY", "1234"])
    ret = operation.handle_req(["GET", "KEY"])
    assert(ret == ["$4\r\n1234\r\n"])


def test_key_get_none():
    ret = operation.handle_req(["GET", "KEY_NIL"])
    assert(ret == ["$-1\r\n"])


def test_key_expire_ttl():
    operation.handle_req(["SET", "KEY_TTL", "123"])
    ret = operation.handle_req(["TTL", "KEY_NIL"])
    assert(ret == [":-2\r\n"])

    ret = operation.handle_req(["TTL", "KEY_TTL"])
    assert(ret == [":-1\r\n"])

    ret = operation.handle_req(["EXPIRE", "KEY_TTL_NIL", "1000"])
    assert(ret == [":0\r\n"])

    ret = operation.handle_req(["EXPIRE", "KEY_TTL", "1000"])
    assert(ret == [":1\r\n"])

    ret = operation.handle_req(["TTL", "KEY_TTL"])
    assert(ret == [":1000\r\n"])

    import time
    ret = operation.handle_req(["EXPIREAT", "KEY_TTL", "%s" % (time.time() + 999)])
    assert(ret == [":1\r\n"])

    ret = operation.handle_req(["TTL", "KEY_TTL"])
    assert(ret == [":999\r\n"] or ret == [":998\r\n"])


def test_key_pexpire_pttl():
    operation.handle_req(["SET", "KEY_PTTL", "123"])
    ret = operation.handle_req(["TTL", "KEY_NIL"])
    assert(ret == [":-2\r\n"])

    ret = operation.handle_req(["TTL", "KEY_PTTL"])
    assert(ret == [":-1\r\n"])

    ret = operation.handle_req(["PEXPIRE", "KEY_PTTL", "1000"])
    assert(ret == [":1\r\n"])

    ret = operation.handle_req(["PTTL", "KEY_PTTL"])
    assert(ret == [":1000\r\n"])


def test_key_pexpireat_pttl():
    import time
    operation.handle_req(["SET", "KEY_PTTL_AT", "123"])
    ret = operation.handle_req(["PEXPIREAT", "KEY_PTTL_AT", "%s" % (time.time()*1000 + 999)])
    assert(ret == [":1\r\n"])

    ret = operation.handle_req(["PTTL", "KEY_PTTL_AT"])
    assert(ret and (int(ret[0][1:].strip()) < 1010 ))


