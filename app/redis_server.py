import asyncio
import logging

from app.resp_parser import RespParser
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_server")


class RedisServer:
    @classmethod
    async def new(cls):
        self = cls()
        self.server = await asyncio.start_server(
            RedisServer.connection_handler, "localhost", 6379
        )
        logger.info("creating server")
        return self

    async def connection_handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
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
                resp = await RedisServer.handle_request(req)
                if not resp:
                    continue
                writer.write(resp)
                logger.info(f"replied {resp} to client")
                await writer.drain()

    async def handle_request(req: List[bytes]) -> bytes:
        assert len(req) > 0
        match req[0].upper():
            case b"PING":
                return b"+PONG\r\n"
            case b"ECHO":
                msg: bytes = req[1]
                msg_len = len(msg)
                return b"$" + str(msg_len).encode("utf-8") + b"\r\n" + msg + b"\r\n"
            case _:
                logger.error(f"{req[0]} not implemented yet!")
                return b"+PONG\r\n"

    async def serve(self):
        async with self.server:
            await self.server.serve_forever()
