from typing import List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("resp_parser")


class RespParser:
    def parse_request(req: bytes) -> List[bytes | List[bytes]]:
        parsed_cmds = []
        while req:
            cmd, req = RespParser._parse_request(req)
            parsed_cmds.append(cmd)
        return parsed_cmds

    def _parse_request(req: bytes) -> tuple[bytes | List[bytes], bytes]:
        match req[:1]:
            case b"+":
                return RespParser.parse_simplestr(req)
            case b"$":
                return RespParser.parse_bulkstr(req)
            case b"*":
                return RespParser.parse_array(req)
            case _:
                logger.info(f"Invalid resp type. Request: {req}")
                return (b"", req)

    def parse_simplestr(req: bytes) -> tuple[bytes, bytes]:
        # simply returns the data without the type byte and <CR>
        # return [data[1:-2]]
        assert req[:1] == b"+"
        req = req.lstrip(b"+")
        data, remain = req.split(b"\r\n", maxsplit=1)
        return (data, remain)

    def parse_bulkstr(req: bytes) -> tuple[bytes, bytes]:
        assert req[:1] == b"$"
        req = req.lstrip(b"$")
        (length, data, remain) = req.split(b"\r\n", maxsplit=2)
        assert int(length) == len(data)
        return (data, remain)

    def parse_array(req) -> tuple[List[bytes], bytes]:
        assert req[:1] == b"*"
        req = req.lstrip(b"*")
        arr_len, req = req.split(b"\r\n", maxsplit=1)
        arr_len = int(arr_len)
        arr = []
        for i in range(arr_len):
            print("remaining request:", req)
            cmd, req = RespParser._parse_request(req)
            arr.append(cmd)
        return (arr, req)
