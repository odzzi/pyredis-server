import operation


def test_key_set():
    ret = operation.do_set(["SET", "KEY", "123"])
    assert(ret == "$2\r\nOK\r\n")