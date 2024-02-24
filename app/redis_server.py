import asyncio
import logging

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

    @classmethod
    async def connection_handler(cls, reader, writer):
        while True:
            data = await reader.read(1024)
            message = data.decode()
            addr = writer.get_extra_info("peername")
            logger.info(f"received {message!r} from {addr!r}")
            writer.write(b"+PONG\r\n")
            logger.info("replied PONG to client")
            await writer.drain()

    async def serve(self):
        async with self.server:
            await self.server.serve_forever()
