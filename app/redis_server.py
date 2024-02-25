import asyncio
import logging
import time

from app.resp_parser import RespParser
from typing import List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_server")


class RedisServer:
    @classmethod
    async def new(cls):
        self = cls()
        self.server = await asyncio.start_server(
            self.connection_handler, "localhost", 6379
        )
        logger.info("creating server")
        self.kvstore = {}
        return self

    async def connection_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        while True:
            data: bytes = await reader.read(1024)
            addr: str = writer.get_extra_info("peername")

            if not data:
                logger.info(f"closing connection with {addr}")
                writer.close()
                await writer.wait_closed()
                return

            requests = RespParser.parse_request(data)
            if requests:
                logger.info(f"received {requests!r} from {addr!r}")
            for req in requests:
                resp = await self.handle_request(req)
                if not resp:
                    continue
                writer.write(resp)
                logger.info(f"replied {resp} to client")
                await writer.drain()

    async def handle_request(self, req: List[bytes]) -> bytes:
        assert len(req) > 0
        match req[0].upper():
            case b"PING":
                return b"+PONG\r\n"
            case b"ECHO":
                if len(req) < 1:
                    return b"-Missing argument(s) for ECHO\r\n"
                msg: bytes = req[1]
                msg_len = len(msg)
                return b"$" + str(msg_len).encode("utf-8") + b"\r\n" + msg + b"\r\n"
            case b"SET":
                if len(req) < 2:
                    return b"-Missing argument(s) for SET\r\n"
                key: bytes = req[1]
                val: bytes = req[2]
                expiry: int = -1
                if len(req) > 3:
                    precision: bytes = req[3]
                    if precision.upper() != b"PX":
                        return b"-Currently only supporting PX for SET timeout\r\n"
                    expiry = time.time_ns() // 1_000_000 + int(req[4].decode())
                self.kvstore[key] = (val, expiry)
                return b"+OK\r\n"
            case b"GET":
                if len(req) < 2:
                    return b"-Missing argument(s) for GET\r\n"
                key: bytes = req[1]
                self.kvstore.get
                value: Tuple[bytes, int] | None = self.kvstore.get(key)
                if value:
                    msg, expiry = value
                    if expiry == -1 or time.time_ns() // 1_000_000 <= expiry:
                        msg_len = len(msg)
                        return (
                            b"$"
                            + str(msg_len).encode("utf-8")
                            + b"\r\n"
                            + msg
                            + b"\r\n"
                        )
                return b"$-1\r\n"
            case _:
                logger.error(f"Received {req[0]} command (not supported)!")
                return b"-Command not supported yet!\r\n"

    async def serve(self):
        async with self.server:
            await self.server.serve_forever()
