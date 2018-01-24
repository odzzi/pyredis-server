import operation


def test_key_set():
    ret = operation.do_set(["SET", "KEY", "123"])
    assert(ret == ["$2\r\nOK\r\n"])


def test_key_get():
    operation.do_set(["SET", "KEY", "123"])
    ret = operation.do_get(["SET", "KEY"])
    assert(ret == ["$3\r\n123\r\n"])

    operation.do_set(["GET", "KEY", "1234"])
    ret = operation.do_get(["GET", "KEY"])
    assert(ret == ["$4\r\n1234\r\n"])


def test_key_get_none():
    ret = operation.do_get(["GET", "KEY_NIL"])
    assert(ret == ["$-1\r\n"])


