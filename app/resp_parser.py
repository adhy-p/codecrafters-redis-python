from typing import List, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("resp_parser")

# the output could be either:
# 1. a simple type -> bytes
# 2. an aggregate type -> List[simple types] or List[aggregate types]
SIMPLE_RESP = bytes
AGGREGATE_RESP = Union[List[SIMPLE_RESP], List["AGGREGATE_RESP"]]


class RespParser:
    @staticmethod
    def parse_request(req: bytes) -> tuple[List[AGGREGATE_RESP], List[int]]:
        """
        receives stream of bytes that represents a redis command
        a command is represented by an array of bulk strings.
        we assume that the incoming bytes can have two separate commands
        e.g. *1\r\n$4\r\nping\r\n*2\r\n$4\r\necho\r\n$3\r\nhey\r\n

        returns a list of commands, e.g.
        [
            [b'ping'],
            [b'echo', b'hello'],
        ]
        """
        parsed_cmds = []
        request_lens = []
        while req:
            cmd, consumed_req_len, req = RespParser._parse_request(req)
            parsed_cmds.append(cmd)
            request_lens.append(consumed_req_len)
        return (parsed_cmds, request_lens)

    @staticmethod
    def _parse_request(req: bytes) -> tuple[SIMPLE_RESP | AGGREGATE_RESP, int, bytes]:
        match req[:1]:
            case b"+":
                return RespParser.parse_simplestr(req)
            case b"$":
                return RespParser.parse_bulkstr(req)
            case b"*":
                return RespParser.parse_array(req)
            case _:
                logger.info(f"Invalid resp type. Request: {req!r}")
                return (b"", 0, req)

    @staticmethod
    def parse_simplestr(req: bytes) -> tuple[SIMPLE_RESP, int, bytes]:
        # simply returns the data without the type byte and <CR>
        # return [data[1:-2]]
        assert req[:1] == b"+"
        original_len = len(req)
        req = req.lstrip(b"+")
        data, remain = req.split(b"\r\n", maxsplit=1)
        parsed_len = original_len - len(remain)
        return (data, parsed_len, remain)

    @staticmethod
    def parse_bulkstr(req: bytes) -> tuple[SIMPLE_RESP, int, bytes]:
        assert req[:1] == b"$"
        original_len = len(req)
        req = req.lstrip(b"$")
        (length, data, remain) = req.split(b"\r\n", maxsplit=2)
        assert int(length) == len(data)
        parsed_len = original_len - len(remain)
        return (data, parsed_len, remain)

    @staticmethod
    def parse_array(req) -> tuple[AGGREGATE_RESP, int, bytes]:
        assert req[:1] == b"*"
        original_len = len(req)
        req = req.lstrip(b"*")
        remain: bytes
        arr_len, remain = req.split(b"\r\n", maxsplit=1)
        arr_len = int(arr_len)
        arr: List[SIMPLE_RESP | AGGREGATE_RESP] = []
        for i in range(arr_len):
            logger.debug("remaining request:", remain)
            cmd, _, remain = RespParser._parse_request(remain)
            arr.append(cmd)
        parsed_len = original_len - len(remain)
        return (arr, parsed_len, remain)

    @staticmethod
    def parse_rdb(req: bytes) -> tuple[SIMPLE_RESP, int, bytes]:
        assert req[:1] == b"$"
        original_len = len(req)
        req = req.lstrip(b"$")
        (length, remain) = req.split(b"\r\n")
        length = int(length)
        (data, remain) = (remain[:length], remain[length:])
        parsed_len = original_len - len(remain)
        return (data, parsed_len, remain)
