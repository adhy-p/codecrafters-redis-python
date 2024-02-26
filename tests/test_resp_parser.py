from app.resp_parser import RespParser


def test_simplestr():
    req = b"+OK\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [b"OK"]
    assert result[1] == [len(req)]


def test_bulkstr():
    req = b"$5\r\nhello\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [b"hello"]
    assert result[1] == [len(req)]

    req = b"$0\r\n\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [b""]
    assert result[1] == [len(req)]


def test_array():
    req = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [[b"hello", b"world"]]
    assert result[1] == [len(req)]


def test_multiple_requests():
    req = b"+OK\r\n$5\r\nhello\r\n*0\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [b"OK", b"hello", []]
    assert result[1] == [len(b"+OK\r\n"), len(b"$5\r\nhello\r\n"), len(b"*0\r\n")]


def test_nested_array():
    req = b"*2\r\n*2\r\n+Hello\r\n+World\r\n*1\r\n$5\r\nhello\r\n"
    result = RespParser.parse_request(req)
    assert result[0] == [[[b"Hello", b"World"], [b"hello"]]]
    assert result[1] == [len(req)]
