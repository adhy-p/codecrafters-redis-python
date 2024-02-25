from app.resp_parser import RespParser


def test_simplestr():
    req = b"+OK\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 1
    assert result[0] == b"OK"


def test_bulkstr():
    req = b"$5\r\nhello\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 1
    assert result[0] == b"hello"

    req = b"$0\r\n\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 1
    assert result[0] == b""


def test_array():
    req = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 1
    result = result[0]
    assert result[0] == b"hello"
    assert result[1] == b"world"


def test_multiple_requests():
    req = b"+OK\r\n$5\r\nhello\r\n*0\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 3
    assert result[0] == b"OK"
    assert result[1] == b"hello"
    assert result[2] == []


def test_nested_array():
    req = b"*2\r\n*2\r\n+Hello\r\n+World\r\n*1\r\n$5\r\nhello\r\n"
    result = RespParser.parse_request(req)
    assert len(result) == 1
    assert isinstance(result[0], list)
    first_arr = result[0][0]
    assert first_arr[0] == b"Hello"
    assert first_arr[1] == b"World"
    second_arr = result[0][1]
    assert second_arr[0] == b"hello"
